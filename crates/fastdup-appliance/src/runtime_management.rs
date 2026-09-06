//! Serve management independently of checkpoint admission and durability waits.
use std::future::Future;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};

const MAX_REQUESTS: usize = 16;

pub struct ManagementServer {
    task: JoinHandle<()>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ManagementServer {
    pub fn start<F, R>(listener: UnixListener, mut handle: F) -> Self
    where
        F: FnMut(UnixStream) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        let (shutdown, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut requests = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept(), if requests.len() < MAX_REQUESTS => {
                        match accepted {
                            Ok((stream, _)) => { requests.spawn(handle(stream)); }
                            Err(error) => {
                                eprintln!("management_accept_error={error}");
                                break;
                            }
                        }
                    }
                    completed = requests.join_next(), if !requests.is_empty() => {
                        if let Some(Err(error)) = completed {
                            eprintln!("management_task_error={error}");
                        }
                    }
                }
            }
            requests.shutdown().await;
        });
        Self {
            task,
            shutdown: Some(shutdown),
        }
    }

    pub async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = (&mut self.task).await;
    }
}

impl Drop for ManagementServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct CanceledRequest(tokio::sync::mpsc::UnboundedSender<()>);

    impl Drop for CanceledRequest {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[tokio::test]
    async fn requests_are_bounded_and_shutdown_retires_them() {
        let path =
            std::env::temp_dir().join(format!("fastdup-mgmt-bound-{}.sock", std::process::id()));
        let listener = UnixListener::bind(&path).unwrap();
        let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
        let (canceled, mut cancellations) = tokio::sync::mpsc::unbounded_channel();
        let server = ManagementServer::start(listener, move |stream| {
            let started = started.clone();
            let guard = CanceledRequest(canceled.clone());
            async move {
                let _stream = stream;
                let _guard = guard;
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            }
        });
        let mut clients = Vec::new();
        for _ in 0..=MAX_REQUESTS {
            clients.push(UnixStream::connect(&path).await.unwrap());
        }
        for _ in 0..MAX_REQUESTS {
            tokio::time::timeout(Duration::from_secs(1), starts.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), starts.recv())
                .await
                .is_err()
        );
        server.stop().await;
        for _ in 0..MAX_REQUESTS {
            cancellations
                .try_recv()
                .expect("shutdown must retire every accepted request");
        }
        assert!(cancellations.try_recv().is_err());
        drop(clients);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn inspection_accepts_while_supervisor_and_another_client_are_waiting() {
        let path = std::env::temp_dir().join(format!(
            "fastdup-management-isolation-{}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&path).unwrap();
        let server = ManagementServer::start(listener, |mut stream| async move {
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, b"inspect");
            stream.write_all(b"runtime-counters").await.unwrap();
        });
        // A client that has not finished its request cannot block other clients.
        let slow = UnixStream::connect(&path).await.unwrap();
        // The supervisor is awaiting a checkpoint. The separate listener must
        // serve the inspection that allows that simulated checkpoint to finish.
        let (completed, checkpoint) = tokio::sync::oneshot::channel();
        let client_path = path.clone();
        let client = tokio::spawn(async move {
            let mut stream = UnixStream::connect(client_path).await.unwrap();
            stream.write_all(b"inspect").await.unwrap();
            stream.shutdown().await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"runtime-counters");
            completed.send(()).unwrap();
        });
        let result = tokio::time::timeout(Duration::from_millis(400), checkpoint).await;
        server.stop().await;
        drop(slow);
        std::fs::remove_file(path).unwrap();
        result
            .expect("inspection must meet the sampler deadline during checkpoint wait")
            .unwrap();
        client.await.unwrap();
    }
}
