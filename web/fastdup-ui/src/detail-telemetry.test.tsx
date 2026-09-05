import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { DetailTelemetryPanel, type DetailTelemetry } from "./detail-telemetry";
import { I18nProvider } from "./i18n";
import { previewSnapshot } from "./types";
vi.mock("echarts-for-react", () => ({ default: () => <div data-testid="phase-chart" /> }));
afterEach(cleanup);
const operation = {operations:100,errors:2,p50Micros:500,p95Micros:2500,p99Micros:10000};
const details: DetailTelemetry = {latency:{read:operation,write:{...operation,operations:0,errors:0}},runtime:{runtimeId:"test",ioUring:{ringEntries:64,inflightBytes:1000000,maxInflightBytes:8000000,peakInflightBytes:4000000,submitted:18,completed:16},caches:[{id:"verifiedRead",hits:75,misses:25,evictions:3,residentBytes:1024},{id:"exactIndex",hits:0,misses:0,evictions:0,residentPages:0}],reduction:{enabled:true,queries:7,candidates:4,acceptedPrefixes:2,acceptedSparseXor:1,savedPayloadBytes:5000,fallbacks:4,errors:0},gc:{state:"collected",observedAt:100,totalMs:12,unlinkedBytes:8000},checkpoint:{completedAt:100,generation:8,totalMs:10,phases:[{id:"freeze",wallMs:2,cpuMs:1}]}}};
it("renders real counters, distinguishes no samples, and switches all five detail views", () => {
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details}} historical={false} loading={false}/></I18nProvider>);
 expect(screen.getByText('0,5 ms')).toBeVisible();
 const write = screen.getByRole('row',{name:/Write/});expect(within(write).getAllByText('—')).toHaveLength(3);
 fireEvent.click(screen.getByRole('tab',{name:'io_uring'}));expect(screen.getByText('1 MB')).toBeVisible();
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));expect(screen.getByText('75 %')).toBeVisible();expect(within(screen.getByRole('row',{name:/Exact Index/})).getByText('—')).toBeVisible();
 fireEvent.click(screen.getByRole('tab',{name:'GC & Reduction'}));expect(screen.getByText('8 KB')).toBeVisible();expect(screen.getByText('5 KB')).toBeVisible();
 fireEvent.click(screen.getByRole('tab',{name:'Checkpoint-Phasen'}));expect(screen.getByTestId('phase-chart')).toBeVisible();expect(screen.getByText('2 ms')).toBeVisible();
 fireEvent.keyDown(screen.getByRole('tab',{name:'Checkpoint-Phasen'}),{key:'Home'});expect(screen.getByRole('tab',{name:'Latenzen'})).toHaveFocus();
});
it("never substitutes live details for an empty historical interval", () => {
 render(<I18nProvider><DetailTelemetryPanel historical={true} loading={false}/></I18nProvider>);
 expect(screen.getByText('Keine Messwerte im gewählten Zeitraum.')).toBeVisible();
 expect(screen.queryByRole('table')).not.toBeInTheDocument();
});
