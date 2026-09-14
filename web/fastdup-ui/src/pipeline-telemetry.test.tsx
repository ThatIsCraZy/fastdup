import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { PipelineTelemetryPanel, type PipelineTelemetry } from "./pipeline-telemetry";
import { DetailTelemetryPanel, type DetailTelemetry } from "./detail-telemetry";
import { previewSnapshot } from "./types";

vi.mock("echarts-for-react", () => ({ default: ({option}: {option: {yAxis: {data: string[]}}}) => <div data-testid="chart-labels">{option.yAxis.data.join(",")}</div> }));
afterEach(cleanup);

const pipeline: PipelineTelemetry = {
  admission: {open:false,reason:"checkpointTimeout",closures:2,closedMs:26000,currentClosedMs:12000,maximumClosedMs:14000},
  operations: [
    {id:"exactEnqueue",active:3,completed:9,totalMs:2100,maximumMs:2000,busyMs:8000},
    {id:"checkpointPublicationRetire",active:1,completed:0,totalMs:0,maximumMs:0,busyMs:11000},
  ],
};

it("shows unfinished waits, completed-only averages and the closure duration", () => {
  const {rerender} = render(<PipelineTelemetryPanel pipeline={pipeline} />);
  expect(screen.getByText("Schreibannahme zum Messpunkt: Gesperrt")).toBeVisible();
  expect(screen.getByText("Checkpoint über fünf Sekunden")).toBeVisible();
  expect(screen.getByText("12 s")).toBeVisible();
  const active = within(screen.getByRole("table", {name:"Laufende Pipeline-Phasen"}));
  expect(active.getByRole("row", {name:"Exact · Queue-Platz 3 8 s"})).toBeVisible();
  fireEvent.click(screen.getByText("Pipeline-Zeiten seit Mount"));
  const cumulative = within(screen.getByRole("table", {name:"Kumulative Pipeline-Zeiten"}));
  expect(within(cumulative.getByRole("row", {name:/Auf Publikationsabschluss warten/})).getAllByText("—")).toHaveLength(2);
  rerender(<PipelineTelemetryPanel pipeline={{...pipeline, admission:{...pipeline.admission,open:true,reason:null,currentClosedMs:0},operations:pipeline.operations.map(operation=>({...operation,active:0,busyMs:0}))}} />);
  expect(screen.getByText("Schreibannahme zum Messpunkt: Offen")).toBeVisible();
  expect(screen.getByText("26 s")).toBeVisible();
  expect(screen.queryByRole("table", {name:"Laufende Pipeline-Phasen"})).not.toBeInTheDocument();
});

it("retains the sampled state in history and avoids double counting nested checkpoint phases", () => {
  const runtime: NonNullable<DetailTelemetry["runtime"]> = {
    runtimeId:"test",pipeline,ioUring:{ringEntries:1,inflightBytes:0,maxInflightBytes:1,peakInflightBytes:0,submitted:0,completed:0},caches:[],
    reduction:{enabled:false,queries:0,candidates:0,acceptedPrefixes:0,acceptedSparseXor:0,savedPayloadBytes:0,fallbacks:0,errors:0},
  };
  const {rerender} = render(<DetailTelemetryPanel sample={{...previewSnapshot.telemetry, details:{runtime}}} historical loading={false} initialTab={5} />);
  expect(screen.getByText("12 s")).toBeVisible();
  expect(screen.getByText("Seit dem Mount wurde noch kein Checkpoint abgeschlossen.")).toBeVisible();
  rerender(<DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:{runtime:{...runtime,checkpoint:{completedAt:100,generation:8,totalMs:16,unattributedMs:1,phases:[{id:"manifestPlan",wallMs:10,cpuMs:2},{id:"cdc",wallMs:9,cpuMs:1},{id:"publicationWait",wallMs:5,cpuMs:0}]}}}}} historical loading={false} initialTab={5} />);
  expect(screen.getByTestId("chart-labels")).toHaveTextContent("Manifest planen · gesamt,Auf Publikation am Cut warten,Sonstige Verwaltung");
  expect(screen.getByTestId("chart-labels")).not.toHaveTextContent("CDC");
});
