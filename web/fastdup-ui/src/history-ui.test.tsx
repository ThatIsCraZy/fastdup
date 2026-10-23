import {act, cleanup, fireEvent, render, screen, waitFor} from "@testing-library/react";
import {afterEach, beforeEach, expect, it, vi} from "vitest";
import {App} from "./App";
import {previewSnapshot, type TelemetrySnapshot} from "./types";

vi.mock("echarts-for-react", () => ({default: ({onEvents}: {onEvents?: {click: (event: {dataIndex:number}) => void}}) => <div data-testid="history-chart" onClick={() => onEvents?.click({dataIndex:0})}/> }));
afterEach(() => {cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals();});

const old: TelemetrySnapshot = {...previewSnapshot.telemetry,sequence:1,observedAt:"2026-10-22T12:00:00Z",frontendWriteMbps:1,
  disks:[{...previewSnapshot.telemetry.disks[0],model:"Disk-at-dip"}]};
const recent: TelemetrySnapshot = {...old,sequence:2,observedAt:"2026-10-22T12:10:00Z",frontendWriteMbps:100,
  disks:[{...old.disks[0],model:"Disk-after-dip"}]};
let samples = [old,recent];
let historyRequest: ((input: string) => Promise<Response>) | undefined;
beforeEach(() => {
  samples = [old,recent]; historyRequest = undefined;
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/telemetry/history?")) return historyRequest?.(url) ?? Promise.resolve(new Response(JSON.stringify(samples)));
    const body = url.endsWith("/session") ? {username:"admin",csrfToken:"csrf",mustChangePassword:false,certificateFingerprint:"AA"}
      : url.endsWith("/principals") ? {users:[],groups:[]}
      : url.includes("/telemetry/events?") ? {events:[{id:"audit:1",timestamp:1792670400,source:"audit",action:"checkpoint",outcome:"failed",detail:"historical-event"}],truncated:false}
      : previewSnapshot;
    return Promise.resolve(new Response(JSON.stringify(body)));
  }));
});
async function openHistory() {
  render(<App/>);
  fireEvent.click(await screen.findByRole("button",{name:"Telemetrie"}));
  fireEvent.click(screen.getByRole("button",{name:"24 h"}));
  await screen.findByText("Disk-after-dip");
}

it("selects the same historical sample from either chart and via keyboard controls", async () => {
  await openHistory();
  fireEvent.click(screen.getAllByTestId("history-chart")[0]);
  expect(screen.getByText("Disk-at-dip")).toBeVisible();
  expect(screen.queryByText("Disk-after-dip")).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button",{name:"Nächster Messpunkt"}));
  expect(screen.getByText("Disk-after-dip")).toBeVisible();
  fireEvent.click(screen.getAllByTestId("history-chart")[1]);
  expect(screen.getByRole("slider",{name:"Historischer Messpunkt"})).toHaveValue("0");
  expect(screen.getByText("Disk-at-dip")).toBeVisible();
  expect(screen.getByText("historical-event")).toBeVisible();
});

it("loads a fixed zoom window rather than resetting the selection to now", async () => {
  vi.spyOn(Date,"now").mockReturnValue(Date.parse("2026-10-22T13:00:00Z"));
  await openHistory();
  fireEvent.click(screen.getByRole("button",{name:"Vorheriger Messpunkt"}));
  fireEvent.click(screen.getByRole("button",{name:"Detail-Zoom"}));
  const time = Date.parse(old.observedAt) / 1000;
  await waitFor(() => expect(fetch).toHaveBeenCalledWith(`/api/v1/telemetry/history?from=${time-60}&to=${time+60}&limit=1500`,expect.anything()));
  expect(screen.getByRole("button",{name:"Zurück zum größeren Zeitfenster"})).toBeEnabled();
});

it("ignores an old request that finishes after returning to Live", async () => {
  let resolve: (response: Response) => void = () => {};
  historyRequest = () => new Promise<Response>(done => {resolve = done;});
  render(<App/>);
  fireEvent.click(await screen.findByRole("button",{name:"Telemetrie"}));
  fireEvent.click(screen.getByRole("button",{name:"24 h"}));
  fireEvent.click(screen.getByRole("button",{name:"Live"}));
  await act(async () => resolve(new Response(JSON.stringify(samples))));
  expect(screen.queryByText("Disk-at-dip")).not.toBeInTheDocument();
  expect(screen.queryByRole("slider",{name:"Historischer Messpunkt"})).not.toBeInTheDocument();
});

it("exports the displayed window and keeps the selected original incident", async () => {
  const range = {min:0,max:100,mean:50};
  samples = [old,{...recent, history:{version:"1",from:1792670400,to:1792671000,resolutionSeconds:60,sampleCount:600,extremaComplete:true,read:range,write:range,cpu:range,ram:range,incident:old}}];
  const blob = vi.fn(() => "blob:diagnosis");
  vi.stubGlobal("URL",Object.assign(URL,{createObjectURL:blob,revokeObjectURL:vi.fn()}));
  vi.spyOn(HTMLAnchorElement.prototype,"click").mockImplementation(() => {});
  await openHistory();
  fireEvent.click(screen.getByRole("button",{name:"Auffälligen Snapshot anzeigen"}));
  expect(screen.getByText("Disk-at-dip")).toBeVisible();
  fireEvent.click(screen.getByRole("button",{name:"Diagnosefenster exportieren"}));
  expect(blob).toHaveBeenCalledWith(expect.any(Blob));
});
