import {
  act,
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

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("FastDup Control Plane UI", () => {
  let liveSnapshot = previewSnapshot;
  beforeEach(() => {
    liveSnapshot = previewSnapshot;
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
            : liveSnapshot;
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );
  });

  async function openUiPreferences() {
    fireEvent.click(await screen.findByRole("button", { name: /admin administrator/i }));
    fireEvent.click(screen.getByRole("menuitem", { name: /UI-Einstellungen|UI settings/ }));
  }

  it("zeigt die Reduktionsbasis und fehlende Belegung ohne erfundenen Faktor", async () => {
    const original = globalThis.fetch;
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => String(input).endsWith("/snapshot")
      ? Promise.resolve(new Response(JSON.stringify({...previewSnapshot, telemetry: {...previewSnapshot.telemetry, reductionRatio: null}})))
      : original(input, init)));
    render(<App />);
    await screen.findByText("Exact Dedup · seit Mount");
    expect(screen.getByText("Gesamtreduktion nicht verfügbar")).toBeVisible();
    fireEvent.click(screen.getByRole("button", {name:"Telemetrie"}));
    expect(screen.getByText("Gesamtreduktion · DATA + Metadata")).toBeVisible();
  });

  it("speichert die UI-Sprache per Session und lädt sie nach erneutem Öffnen", async () => {
    let language = "de";
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/session/language")) language = JSON.parse(String(init?.body)).language;
      const body = url.endsWith("/session/language") ? { uiLanguage: language }
        : url.endsWith("/session") ? { username: "admin", csrfToken: "csrf", mustChangePassword: false, certificateFingerprint: "AA", uiLanguage: language }
        : url.endsWith("/principals") ? { users: [], groups: [] } : previewSnapshot;
      return Promise.resolve(new Response(JSON.stringify(body), {status:200, headers:{"content-type":"application/json"}}));
    });
    vi.stubGlobal("fetch", fetchMock);
    const view = render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    expect(screen.queryByRole("combobox", { name: "UI-Sprache" })).not.toBeInTheDocument();
    await openUiPreferences();
    fireEvent.change(await screen.findByRole("combobox", { name: "UI-Sprache" }), { target: { value: "en" } });
    await screen.findByRole("button", { name: "Overview" });
    expect(document.documentElement.lang).toBe("en");
    expect(fetchMock).toHaveBeenCalledWith("/api/v1/session/language", expect.objectContaining({method:"PUT", body:JSON.stringify({language:"en"})}));
    fireEvent.click(screen.getByRole("button", { name: "Close dialog" }));
    fireEvent.click(screen.getByRole("button", { name: "SMB Shares" }));
    fireEvent.click(screen.getByRole("button", { name: "Create share" }));
    expect(screen.getByRole("dialog", { name: "New share" })).toBeVisible();
    view.unmount();
    render(<App />);
    await screen.findByRole("button", { name: "Overview" });
    await openUiPreferences();
    expect(screen.getByRole("combobox", { name: "UI language" })).toHaveValue("en");
  });

  it("behält bei einem Speicherfehler die bisherige UI-Sprache", async () => {
    const original = globalThis.fetch;
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => String(input).endsWith("/session/language")
      ? Promise.resolve(new Response(JSON.stringify({message:"Save failed"}),{status:500}))
      : original(input, init)));
    render(<App />);
    await openUiPreferences();
    fireEvent.change(await screen.findByRole("combobox", { name: "UI-Sprache" }), { target: { value: "en" } });
    await screen.findByText("Sprache konnte nicht gespeichert werden");
    expect(screen.getByRole("button", { name: "Übersicht" })).toBeVisible();
    expect(screen.getByRole("combobox", { name: "UI-Sprache" })).toHaveValue("de");
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

  it("öffnet echte Detailbereiche und erklärt fehlende Runtime-Daten", async () => {
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Telemetrie" }));
    expect(screen.getByRole("tab", { name: "Latenzen" })).toBeVisible();
    expect(screen.getByText("Runtime-Messdaten sind momentan nicht verfügbar. Die Anzeige wird automatisch aktualisiert.")).toBeVisible();
    expect(document.querySelector(".telemetry-tabs")).not.toBeInTheDocument();
  });

  it("sperrt neue Shares ohne Repository und führt zur Einrichtung", async () => {
    const original = globalThis.fetch;
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => String(input).endsWith("/snapshot")
      ? Promise.resolve(new Response(JSON.stringify({...previewSnapshot, repository: undefined})))
      : original(input, init)));
    render(<App />);
    fireEvent.click(await screen.findByRole("button", {name:"SMB-Freigaben"}));
    expect(screen.getByRole("button", {name:"Freigabe anlegen"})).toBeDisabled();
    fireEvent.click(screen.getByRole("button", {name:"Repository einrichten"}));
    expect(screen.getByRole("heading", {name:"Laufwerke & Provisionierung"})).toBeVisible();
  });

  it("öffnet jede Hauptseite mit ihrer echten Live-Ansicht", async () => {
    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    for (const [navigation, heading] of [
      ["Übersicht", "Production Repository"],
      ["Repository", "Production Repository"],
      ["Laufwerke", "Laufwerke & Belegung"],
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
    fireEvent.change(
      screen.getByRole("combobox", { name: /advanced reduction.*repository-standard/i }),
      { target: { value: "dependent_v1" } },
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
    expect(submitted.settings.advancedReduction).toBe("dependent_v1");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
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
    expect(screen.getByText("Zeitraum · 24 h")).toBeVisible();
  });

  it("zeigt historische Kennzahlen und Laufwerke ohne aktuelle Werte beizumischen", async () => {
    const original = globalThis.fetch;
    const past = {...previewSnapshot.telemetry, observedAt:"2026-09-08T12:00:00Z", frontendReadMbps:12.3, frontendWriteMbps:45.6, reductionRatio:8.22, disks:[{...previewSnapshot.telemetry.disks[0],model:"Historisches Laufwerk"}]};
    let resolveHistory: (response: Response) => void = () => {};
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => String(input).includes("/telemetry/history?")
      ? new Promise<Response>(resolve => {resolveHistory=resolve;}) : original(input, init)));
    const {container}=render(<App />);
    await screen.findByText("Exact Dedup · seit Mount");
    fireEvent.click(screen.getByRole("button", {name:"Telemetrie"}));
    expect(screen.getByRole("tab", {name:"Caches"})).toHaveAttribute("aria-selected","true");
    fireEvent.click(screen.getByRole("button", {name:"24 h"}));
    expect(container.querySelector(".telemetry-metrics")).not.toHaveTextContent("842,6");
    await act(async () => {resolveHistory(new Response(JSON.stringify([past])));});
    expect(container.querySelector(".telemetry-metrics")).toHaveTextContent("12,3 MB/s");
    expect(container.querySelector(".telemetry-metrics")).toHaveTextContent("8,22×");
    expect(screen.getByText("Historisches Laufwerk")).toBeVisible();
    expect(screen.queryByText("Micron 7450 MAX")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", {name:"7 d"}));
    await act(async () => {resolveHistory(new Response("[]"));});
    expect(container.querySelector(".telemetry-metrics")).not.toHaveTextContent("12,3");
    expect(screen.queryByText("Historisches Laufwerk")).not.toBeInTheDocument();
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
        readIops: 123.4, writeIops: 56.7,
      },
      {
        ...previewSnapshot.telemetry.disks[1],
        id: "sdb",
        model: "Repo Data Disk",
        readIops: undefined, writeIops: undefined,
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
    expect(screen.getByText("123,4 / 56,7 IOPS")).toBeVisible();
    expect(screen.getByText("— / — IOPS")).toBeVisible();
  });

  it("provisioniert nur über erkannte Target-Karten ohne Gerätepfad-Freitext", async () => {
    liveSnapshot = {...previewSnapshot, repository: undefined};
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
    expect(await screen.findByRole("heading", { name: "Laufwerke & Belegung" })).toBeVisible();
    expect(screen.queryByRole("button", { name: /neues repository initialisieren/i })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /^repository$/i }));
    expect(screen.getAllByRole("button", { name: /offline-scrub/i })[0]).toBeEnabled();
    expect(screen.getAllByRole("button", { name: /unmount/i })[0]).toBeEnabled();
  });

  it("sortiert nicht auswählbare Targets stabil unter auswählbare Targets", async () => {
    const unordered = structuredClone(previewSnapshot);
    delete unordered.repository;
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

  it("öffnet den Share-Editor als benanntes Modal und schließt mit Escape", async () => {
    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /smb-freigaben/i }));
    fireEvent.click(screen.getByRole("button", { name: /freigabe anlegen/i }));
    const dialog = screen.getByRole("dialog", { name: "Neue Freigabe" });
    expect(dialog.tagName).toBe("DIALOG");
    expect(dialog).toHaveAttribute("open");
    expect(document.body.style.overflow).toBe("hidden");
    fireEvent(dialog, new Event("cancel", { bubbles: false, cancelable: true }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(document.body.style.overflow).not.toBe("hidden");
  });

  it("schaltet Similarity pro Freigabe unabhängig vom Repository-Standard", async () => {
    render(<App />);
    await screen.findByRole("button", { name: /admin administrator/i });
    fireEvent.click(screen.getByRole("button", { name: /smb-freigaben/i }));
    fireEvent.click(await screen.findByRole("button", { name: /freigabe anlegen/i }));
    const policy = screen.getByRole("combobox", { name: /advanced reduction für diese freigabe/i });
    expect(policy).toHaveValue("off");
    fireEvent.change(policy, { target: { value: "dependent_v1" } });
    expect(policy).toHaveValue("dependent_v1");
    fireEvent.change(policy, { target: { value: "inherit" } });
    expect(policy).toHaveValue("inherit");
    expect(screen.getByText(/gemeinsamen kandidatenindex/i)).toBeVisible();
  });
});

it("loads inventory after interactive login without a browser reload", async () => {
  let loggedIn = false;
  const fetch = vi.fn((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.endsWith("/session") && !loggedIn) return Promise.resolve(new Response("{}", { status: 401 }));
    if (url.endsWith("/session/login")) loggedIn = true;
    const body = url.endsWith("/session/login") ? { username: "admin", csrfToken: "csrf", mustChangePassword: false }
      : url.endsWith("/principals") ? { users: [], groups: [] } : {...previewSnapshot, repository: undefined};
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
  });
  vi.stubGlobal("fetch", fetch);
  render(<App />);
  fireEvent.change(await screen.findByLabelText("Passwort"), { target: { value: "test-password" } });
  fireEvent.click(screen.getByRole("button", { name: "Anmelden" }));
  fireEvent.click(await screen.findByRole("button", { name: "Laufwerke" }));
  await waitFor(() => expect(document.querySelectorAll(".target-card:enabled").length).toBeGreaterThan(0));
});

it("keeps the quota warning in the topbar across missing online samples and clears it on unmount", async () => {
  const source = new EventTarget();
  const listener = vi.spyOn(EventSource.prototype, "addEventListener").mockImplementation(source.addEventListener.bind(source));
  const quota = { requestedBytes: 68719476736, effectiveBytes: 10737418240 };
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
    const url = String(input);
    const body = url.endsWith("/session") ? { username: "admin", csrfToken: "csrf", mustChangePassword: false }
      : url.endsWith("/principals") ? { users: [], groups: [] }
      : { ...previewSnapshot, telemetry: { ...previewSnapshot.telemetry, repositoryState: "online", smallFileQuota: quota } };
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
  }));
  const view = render(<App />);
  const warning = await screen.findByText(/Small-File-Limit wegen kleinem Metadata-Volume/);
  expect(warning.closest("header")).toHaveClass("topbar");
  expect(warning.closest(".page-content")).toBeNull();
  const sample = (smallFileQuota: typeof quota | null, repositoryState = "online") => act(() => {
    source.dispatchEvent(new MessageEvent("snapshot", { data: JSON.stringify({
      ...previewSnapshot.telemetry, repositoryState, smallFileQuota,
    }) }));
  });
  sample(null);
  expect(warning).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Repository" }));
  expect(warning).toBeInTheDocument();
  sample({ ...quota, effectiveBytes: quota.requestedBytes });
  expect(screen.queryByText(/Small-File-Limit wegen kleinem Metadata-Volume/)).not.toBeInTheDocument();
  sample(quota);
  expect(screen.getByText(/Small-File-Limit wegen kleinem Metadata-Volume/)).toBeInTheDocument();
  sample(null, "unmounted");
  expect(screen.queryByText(/Small-File-Limit wegen kleinem Metadata-Volume/)).not.toBeInTheDocument();
  sample(null);
  expect(screen.queryByText(/Small-File-Limit wegen kleinem Metadata-Volume/)).not.toBeInTheDocument();
  view.unmount();
  listener.mockRestore();
  vi.unstubAllGlobals();
});

it("shows confirmed mount/process failures but keeps missing metrics separate", async () => {
  const source = new EventTarget();
  vi.spyOn(EventSource.prototype, "addEventListener").mockImplementation(source.addEventListener.bind(source));
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
    const url = String(input);
    const body = url.endsWith("/session") ? { username: "admin", csrfToken: "csrf", mustChangePassword: false }
      : url.endsWith("/principals") ? { users: [], groups: [] } : previewSnapshot;
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
  }));
  render(<App />);
  await screen.findByRole("button", { name: "Telemetrie" });
  const sample = (runtimeIssue: string | undefined) => act(() => {
    source.dispatchEvent(new MessageEvent("snapshot", { data: JSON.stringify({
      ...previewSnapshot.telemetry, repositoryState: runtimeIssue ? "error" : "online",
      runtimeIssue, details: runtimeIssue === "unavailable" ? null : previewSnapshot.telemetry.details,
      frontendReadMbps: 0, frontendWriteMbps: 0,
    }) }));
  });
  sample("unavailable");
  expect(screen.getByRole("alert")).toHaveTextContent("Repository-Mount fehlt");
  expect(screen.getByRole("alert").closest("header")).toHaveClass("topbar");
  expect(screen.getByText("Agent verbunden")).toBeVisible();
  expect(screen.queryByText("Live")).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Repository" }));
  expect(document.querySelector(".repo-hero .badge")).toHaveTextContent("Fehler");
  sample("process_exited");
  expect(screen.getByRole("alert")).toHaveTextContent("Repository-Runtime ist abgestürzt");
  sample(undefined);
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(screen.getAllByText("Live").length).toBeGreaterThan(0);
  act(() => source.dispatchEvent(new MessageEvent("snapshot", { data: JSON.stringify({
    ...previewSnapshot.telemetry, repositoryState: "online", details: null,
    frontendReadMbps: 0, frontendWriteMbps: 0,
  }) })));
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(document.querySelector(".repo-hero .badge")).toHaveTextContent("Online");
});
