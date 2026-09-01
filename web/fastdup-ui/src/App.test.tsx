import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import { previewSnapshot } from "./types";

vi.mock("echarts-for-react", () => ({
  default: () => <div data-testid="chart" />,
}));

describe("FastDup Control Plane UI", () => {
  afterEach(cleanup);
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input);
        const body = url.endsWith("/api/v1/session")
          ? {
              username: "admin",
              csrfToken: "csrf",
              mustChangePassword: false,
              certificateFingerprint: "AA:BB",
            }
          : url.endsWith("/api/v1/samba/principals")
            ? { users: ["backup"], groups: ["storage-admins"] }
            : previewSnapshot;
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );
  });

  it("hält Admin und Logout dauerhaft im rechten oberen Account-Menü", async () => {
    render(<App />);
    const account = await screen.findByRole("button", {
      name: /admin administrator/i,
    });
    expect(account.closest("header")).toHaveClass("topbar");
    fireEvent.click(account);
    expect(screen.getByRole("menuitem", { name: /abmelden/i })).toBeVisible();
  });

  it("öffnet jede Hauptseite mit ihrer echten Live-Ansicht", async () => {
    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    for (const [navigation, heading] of [
      ["Übersicht", "Production Repository"],
      ["Repository", "Production Repository"],
      ["Laufwerke", "Laufwerke & Provisionierung"],
      ["SMB-Freigaben", "SMB-Freigaben"],
      ["Telemetrie", "Tiefentelemetrie"],
      ["Ereignisse", "Ereignisse"],
      ["Einstellungen", "Einstellungen"],
    ]) {
      fireEvent.click(screen.getByRole("button", { name: navigation }));
      expect(
        screen.getByRole("heading", { name: heading, level: 1 }),
      ).toBeVisible();
    }
  });

  it("sendet geänderte Small-File-Endungen über die Runtime-Einstellungen", async () => {
    const fetchMock = vi.fn((input: RequestInfo | URL, _init?: RequestInit) => {
      const url = String(input);
      const body = url.endsWith("/api/v1/session")
        ? {
            username: "admin",
            csrfToken: "csrf",
            mustChangePassword: false,
            certificateFingerprint: "AA:BB",
          }
        : url.endsWith("/api/v1/samba/principals")
          ? { users: [], groups: [] }
          : url.endsWith("/api/v1/repository/commands")
            ? {
                id: "settings-job",
                kind: "update_settings",
                state: "queued",
                progressBasisPoints: 0,
                message: "Wartet",
                createdAt: 1,
                updatedAt: 1,
              }
            : previewSnapshot;
      return Promise.resolve(
        new Response(JSON.stringify(body), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: "Einstellungen" }));
    fireEvent.change(
      screen.getByRole("textbox", { name: /small-file-tier.*dateiendungen/i }),
      { target: { value: ".vmdk\n.XML" } },
    );
    fireEvent.click(screen.getByRole("button", { name: /übernehmen/i }));

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/repository/commands",
        expect.objectContaining({ method: "POST" }),
      ),
    );
    const commandCall = fetchMock.mock.calls.find(
      ([input]) => String(input) === "/api/v1/repository/commands",
    );
    const submitted = JSON.parse(String(commandCall?.[1]?.body));
    expect(submitted.settings.smallFileExtensions).toEqual([".vmdk", ".XML"]);
  });

  it("lädt ausgewählte Telemetrie-Zeiträume aus der Historien-API", async () => {
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      const body = url.endsWith("/api/v1/session")
        ? {
            username: "admin",
            csrfToken: "csrf",
            mustChangePassword: false,
            certificateFingerprint: "AA:BB",
          }
        : url.endsWith("/api/v1/samba/principals")
          ? { users: [], groups: [] }
          : url.includes("/api/v1/telemetry/history")
            ? [previewSnapshot.telemetry]
            : previewSnapshot;
      return Promise.resolve(
        new Response(JSON.stringify(body), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /telemetrie/i }));
    fireEvent.click(screen.getByRole("button", { name: "24 h" }));

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/telemetry/history?"),
        expect.anything(),
      ),
    );
    expect(screen.getByText("RANGE · 24 H")).toBeVisible();
  });

  it("exportiert den Audit-Verlauf und bestätigt den Download", async () => {
    const linkClick = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => undefined);
    const createObjectUrl = vi.fn(() => "blob:audit");
    const revokeObjectUrl = vi.fn();
    Object.defineProperty(URL, "createObjectURL", {
      value: createObjectUrl,
      configurable: true,
    });
    Object.defineProperty(URL, "revokeObjectURL", {
      value: revokeObjectUrl,
      configurable: true,
    });
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input);
        const body = url.endsWith("/api/v1/session")
          ? {
              username: "admin",
              csrfToken: "csrf",
              mustChangePassword: false,
              certificateFingerprint: "AA:BB",
            }
          : url.endsWith("/api/v1/samba/principals")
            ? { users: [], groups: [] }
            : url.endsWith("/api/v1/audit")
              ? [
                  {
                    id: 1,
                    timestamp: 1,
                    actor: "admin",
                    action: "mount",
                    outcome: "accepted",
                    detail: "job-1",
                  },
                ]
              : previewSnapshot;
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );

    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /ereignisse/i }));
    fireEvent.click(screen.getByRole("button", { name: /audit exportieren/i }));

    expect(await screen.findByRole("status")).toHaveTextContent(
      /audit exportiert.*1 audit-einträge/i,
    );
    expect(createObjectUrl).toHaveBeenCalledOnce();
    expect(revokeObjectUrl).toHaveBeenCalledWith("blob:audit");
    expect(linkClick).toHaveBeenCalledOnce();
    linkClick.mockRestore();
  });

  it("zeigt ohne Live-Snapshot niemals Preview-Targets oder Preview-Shares", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input);
        if (url.endsWith("/api/v1/session")) {
          return Promise.resolve(
            new Response(
              JSON.stringify({
                username: "admin",
                csrfToken: "csrf",
                mustChangePassword: false,
                certificateFingerprint: "AA:BB",
              }),
              { status: 200, headers: { "content-type": "application/json" } },
            ),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify({ message: "Live-Daten fehlen" }), {
            status: 503,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );

    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /laufwerke/i }));
    await waitFor(() =>
      expect(
        screen.queryAllByText(previewSnapshot.targets[0].model),
      ).toHaveLength(0),
    );
    fireEvent.click(screen.getByRole("button", { name: /smb-freigaben/i }));
    expect(
      screen.queryAllByText(previewSnapshot.shares[0].name),
    ).toHaveLength(0);
  });

  it("verwechselt die Telemetrie-Sequenz ohne Repository nicht mit einer Generation", async () => {
    const uninitialized = structuredClone(previewSnapshot);
    delete uninitialized.repository;
    uninitialized.telemetry.repositoryState = "uninitialized";
    uninitialized.telemetry.sequence = 216;
    uninitialized.telemetry.commitGeneration = undefined;
    uninitialized.telemetry.lastCheckpointSeconds = undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input);
        const body = url.endsWith("/api/v1/session")
          ? {
              username: "admin",
              csrfToken: "csrf",
              mustChangePassword: false,
              certificateFingerprint: "AA:BB",
            }
          : url.endsWith("/api/v1/samba/principals")
            ? { users: [], groups: [] }
            : uninitialized;
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );

    render(<App />);
    expect(await screen.findByText("Generation —")).toBeVisible();
    expect(screen.getByText("Checkpoint —")).toBeVisible();
    expect(screen.queryByText("Generation 216")).not.toBeInTheDocument();
    expect(screen.queryByText("Checkpoint vor 0 s")).not.toBeInTheDocument();
  });

  it("zeigt Outstanding I/O nur für physische Disks des Repositorys", async () => {
    const live = structuredClone(previewSnapshot);
    live.targets[0].backingDisks = [
      {
        stableId: "meta-physical",
        kernelName: "nvme0n1",
        model: "Repo Metadata Disk",
        serial: "META",
        hbaPort: "PCIe 1",
      },
    ];
    live.targets[1].backingDisks = [
      {
        stableId: "data-physical",
        kernelName: "sdb",
        model: "Repo Data Disk",
        serial: "DATA",
        hbaPort: "SAS 2",
      },
    ];
    live.telemetry.disks = [
      {
        ...previewSnapshot.telemetry.disks[0],
        id: "nvme0n1",
        model: "Repo Metadata Disk",
      },
      {
        ...previewSnapshot.telemetry.disks[1],
        id: "sdb",
        model: "Repo Data Disk",
      },
      {
        ...previewSnapshot.telemetry.disks[0],
        id: "sda",
        role: "System",
        model: "Host System Disk",
      },
    ];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input);
        const body = url.endsWith("/api/v1/session")
          ? {
              username: "admin",
              csrfToken: "csrf",
              mustChangePassword: false,
              certificateFingerprint: "AA:BB",
            }
          : url.endsWith("/api/v1/samba/principals")
            ? { users: [], groups: [] }
            : live;
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );

    render(<App />);
    expect(await screen.findByText("Repo Metadata Disk")).toBeVisible();
    expect(screen.getByText("Repo Data Disk")).toBeVisible();
    expect(screen.queryByText("Host System Disk")).not.toBeInTheDocument();
  });

  it("provisioniert nur über erkannte Target-Karten ohne Gerätepfad-Freitext", async () => {
    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /laufwerke/i }));
    await waitFor(() =>
      expect(screen.getByText("Laufwerke & Provisionierung")).toBeVisible(),
    );
    expect(
      screen.queryByRole("textbox", { name: /gerätepfad/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.getAllByRole("button", { name: /Micron 7450 MAX/i }).length,
    ).toBeGreaterThan(0);
  });

  it("zeigt nach bestätigter Provisionierung sofort Job-Feedback", async () => {
    const uninitialized = structuredClone(previewSnapshot);
    delete uninitialized.repository;
    uninitialized.telemetry.repositoryState = "uninitialized";
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const body = url.endsWith("/api/v1/session")
        ? {
            username: "admin",
            csrfToken: "csrf",
            mustChangePassword: false,
            certificateFingerprint: "AA:BB",
          }
        : url.endsWith("/api/v1/samba/principals")
          ? { users: [], groups: [] }
          : url.endsWith("/api/v1/repository/commands") &&
              init?.method === "POST"
            ? {
                id: "provision-job",
                kind: "provision",
                state: "queued",
                progressBasisPoints: 0,
                message: "Wartet auf Ausführung",
                createdAt: 1,
                updatedAt: 1,
              }
            : uninitialized;
      return Promise.resolve(
        new Response(JSON.stringify(body), {
          status: url.endsWith("/api/v1/repository/commands") ? 202 : 200,
          headers: { "content-type": "application/json" },
        }),
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    const { container } = render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /laufwerke/i }));
    const targetLists = await waitFor(() => {
      const lists = container.querySelectorAll(".target-list");
      expect(lists).toHaveLength(2);
      return lists;
    });
    fireEvent.click(targetLists[0].querySelectorAll("button")[0]);
    fireEvent.click(targetLists[1].querySelectorAll("button")[1]);
    fireEvent.click(
      screen.getByRole("button", { name: /neues repository initialisieren/i }),
    );
    fireEvent.click(
      screen.getByRole("button", { name: /löschen & initialisieren/i }),
    );

    expect(await screen.findByRole("status")).toHaveTextContent(
      /provisionierung.*gestartet|wartet auf ausführung/i,
    );
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/repository/commands",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("zeigt API-Fehler einer Managementaktion sichtbar an", async () => {
    const uninitialized = structuredClone(previewSnapshot);
    delete uninitialized.repository;
    uninitialized.telemetry.repositoryState = "uninitialized";
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (
          url.endsWith("/api/v1/repository/commands") &&
          init?.method === "POST"
        ) {
          return Promise.resolve(
            new Response(
              JSON.stringify({ message: "Provisionierung fehlgeschlagen" }),
              {
                status: 409,
                headers: { "content-type": "application/json" },
              },
            ),
          );
        }
        const body = url.endsWith("/api/v1/session")
          ? {
              username: "admin",
              csrfToken: "csrf",
              mustChangePassword: false,
              certificateFingerprint: "AA:BB",
            }
          : url.endsWith("/api/v1/samba/principals")
            ? { users: [], groups: [] }
            : uninitialized;
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );

    const { container } = render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /laufwerke/i }));
    const targetLists = await waitFor(() => container.querySelectorAll(".target-list"));
    fireEvent.click(targetLists[0].querySelectorAll("button")[0]);
    fireEvent.click(targetLists[1].querySelectorAll("button")[1]);
    fireEvent.click(
      screen.getByRole("button", { name: /neues repository initialisieren/i }),
    );
    fireEvent.click(
      screen.getByRole("button", { name: /löschen & initialisieren/i }),
    );

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Provisionierung fehlgeschlagen",
    );
  });

  it("sperrt unpassende Repository-Aktionen anhand des Live-Zustands", async () => {
    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /laufwerke/i }));
    expect(
      screen.getByRole("button", { name: /neues repository initialisieren/i }),
    ).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: /^repository$/i }));
    expect(screen.getAllByRole("button", { name: /offline-scrub/i })[0]).toBeEnabled();
    expect(screen.getAllByRole("button", { name: /unmount/i })[0]).toBeEnabled();
  });

  it("sortiert nicht auswählbare Targets stabil unter auswählbare Targets", async () => {
    const unordered = structuredClone(previewSnapshot);
    unordered.targets = [
      unordered.targets[2],
      unordered.targets[0],
      unordered.targets[1],
    ];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input);
        const body = url.endsWith("/api/v1/session")
          ? {
              username: "admin",
              csrfToken: "csrf",
              mustChangePassword: false,
              certificateFingerprint: "AA:BB",
            }
          : url.endsWith("/api/v1/samba/principals")
            ? { users: [], groups: [] }
            : unordered;
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );

    const { container } = render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /laufwerke/i }));
    const firstList = await waitFor(() => {
      const list = container.querySelector(".target-list");
      expect(list).not.toBeNull();
      return list as HTMLElement;
    });
    const cards = Array.from(firstList.querySelectorAll("button"));
    expect(cards).toHaveLength(3);
    expect(cards[0]).toBeEnabled();
    expect(cards[1]).toBeEnabled();
    expect(cards[2]).toBeDisabled();
  });

  it("bietet pro Share eine harte logische Quota mit GB-TB-PB-Auswahl an", async () => {
    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /smb-freigaben/i }));
    fireEvent.click(
      await screen.findByRole("button", { name: /freigabe anlegen/i }),
    );

    const value = screen.getByRole("spinbutton", { name: /kapazitätswert/i });
    const unit = screen.getByRole("combobox", { name: /kapazitätseinheit/i });
    expect(value).toBeDisabled();
    fireEvent.change(unit, { target: { value: "pb" } });
    fireEvent.change(value, { target: { value: "12" } });
    expect(value).toHaveValue(12);
    expect(unit).toHaveValue("pb");
    expect(screen.getByText(/harte logische quota/i)).toBeVisible();
  });
});
