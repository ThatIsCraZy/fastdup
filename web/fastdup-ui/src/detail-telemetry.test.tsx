import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { DetailTelemetryPanel, type DetailTelemetry } from "./detail-telemetry";
import { I18nProvider } from "./i18n";
import { previewSnapshot } from "./types";
vi.mock("echarts-for-react", () => ({ default: () => <div data-testid="phase-chart" /> }));
afterEach(cleanup);

it("distinguishes metadata API reads from physical IO and does not invent mmap timings", () => {
 const row={reason:"indexLookup",object:"exactIndex",mode:"bufferedRange",operations:10,requestedBytes:40960,returnedBytes:36864,errors:1,elapsedMicros:10000,maxMicros:3000,inFlight:1,operationsPerSecond:5,requestedMbps:0.02048};
 const metadataReads={intervalSeconds:2,rows:[row,{...row,reason:"indexAudit",mode:"mmap",inFlight:0}]};
 const value={...details,runtime:{...details.runtime!,metadataReads}};
 const {rerender}=render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:value}} historical={false} loading={false}/></I18nProvider>);
 fireEvent.click(screen.getByRole('tab',{name:'Metadata-Reads'}));
 expect(screen.getByText(/Angeforderte Bereiche enthalten keinen Ausrichtungs-/)).toBeVisible();
 fireEvent.click(screen.getByRole('checkbox',{name:'Summen und Lesezeiten seit Mount anzeigen'}));
 expect(within(screen.getByRole('row',{name:/Index-Abfrage/})).getByText('3 ms')).toBeVisible();
 expect(within(screen.getByRole('row',{name:/Index-Prüfung/})).getAllByText('—')).toHaveLength(3);
 rerender(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:{...value,runtime:{...value.runtime,metadataReads:{...metadataReads,intervalSeconds:0}}}}} historical={true} loading={false}/></I18nProvider>);
 expect(screen.getByText('Erster Messpunkt · Raten noch nicht verfügbar')).toBeVisible();
 expect(within(screen.getByRole('row',{name:/Index-Prüfung/})).getAllByText('—')).toHaveLength(5);
});

it("separates allocation reuse from disk-saving cache hit rates", () => {
 const buffers = {id:"codecBuffers",fallbackTier:"memory",hits:1999,misses:1,evictions:0,residentBytes:262144,targetBytes:524288,leasedBytes:524288};
 const sampleDetails: DetailTelemetry = {...details,runtime:{...details.runtime!,
  codecBuffers:{retainedBytes:262144,activeBytes:65536,peakActiveBytes:131072,hits:1999,misses:1,evictions:0},
  cacheBudget:{maximumMemoryUsedBasisPoints:9200,effectiveLimitBytes:1e9,availableBytes:5e8,budgetBytes:4e8,pools:[buffers]}
 }};
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:sampleDetails}} historical={false} loading={false}/></I18nProvider>);
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));
 fireEvent.click(screen.getByText('Wiederverwendbare Codec-Puffer'));
 expect(screen.getByText('Puffer wiederverwendet')).toBeVisible();
 expect(screen.getByText('1.999')).toBeVisible();
 expect(screen.queryByRole('row',{name:/codecBuffers/})).not.toBeInTheDocument();
});
const operation = {operations:100,errors:2,p50Micros:500,p95Micros:2500,p99Micros:10000};
const details: DetailTelemetry = {latency:{read:operation,write:{...operation,operations:0,errors:0}},runtime:{runtimeId:"test",ioUring:{ringEntries:64,inflightBytes:1000000,maxInflightBytes:8000000,peakInflightBytes:4000000,submitted:18,completed:16},caches:[{id:"verifiedRead",hits:75,misses:25,evictions:3,residentBytes:1024},{id:"exactIndex",hits:0,misses:0,evictions:0,residentPages:0}],reduction:{enabled:true,queries:7,candidates:4,acceptedPrefixes:2,acceptedSparseXor:1,savedPayloadBytes:5000,fallbacks:4,errors:0},gc:{state:"collected",observedAt:100,totalMs:12,unlinkedBytes:8000},checkpoint:{completedAt:100,generation:8,totalMs:10,phases:[{id:"freeze",wallMs:2,cpuMs:1}]}}};
it("renders real counters, distinguishes no samples, and switches all six detail views", () => {
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details}} historical={false} loading={false}/></I18nProvider>);
 expect(screen.getByText('0,5 ms')).toBeVisible();
 const write = screen.getByRole('row',{name:/Write/});expect(within(write).getAllByText('—')).toHaveLength(3);
 fireEvent.click(screen.getByRole('tab',{name:'io_uring'}));expect(screen.getByText('1 MB')).toBeVisible();
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));expect(screen.getByText('75 %')).toBeVisible();expect(within(screen.getByRole('row',{name:/Exact Index/})).getByText('—')).toBeVisible();
 fireEvent.click(screen.getByRole('tab',{name:'Lesevermeidung'}));expect(screen.getByText('5 KB')).toBeVisible();
 fireEvent.click(screen.getByRole('tab',{name:'GC & Scrub'}));expect(screen.getByText('8 KB')).toBeVisible();
 fireEvent.click(screen.getByRole('tab',{name:'Checkpoint-Phasen'}));expect(screen.getByTestId('phase-chart')).toBeVisible();expect(screen.getByText('2 ms')).toBeVisible();
 fireEvent.keyDown(screen.getByRole('tab',{name:'Checkpoint-Phasen'}),{key:'Home'});expect(screen.getByRole('tab',{name:'Latenzen'})).toHaveFocus();
});
it("never substitutes live details for an empty historical interval", () => {
 render(<I18nProvider><DetailTelemetryPanel historical={true} loading={false}/></I18nProvider>);
 expect(screen.getByText('Keine Messwerte im gewählten Zeitraum.')).toBeVisible();
 expect(screen.queryByRole('table')).not.toBeInTheDocument();
});

it("shows the shared budget, DATA priority tier and pending donor reservation in historical samples", () => {
 const runtime = details.runtime!;
 const cached = {...details, runtime:{...runtime,cacheBudget:{maximumMemoryUsedBasisPoints:9200,effectiveLimitBytes:20000000000,availableBytes:2000000000,budgetBytes:16000000000,pools:[
  {id:"verifiedRead",fallbackTier:"data",hits:90,misses:10,evictions:5,residentBytes:6000000000,targetBytes:4000000000,leasedBytes:6000000000},
  {id:"historicalProofs",fallbackTier:"data",hits:0,misses:0,evictions:0,residentBytes:0,targetBytes:1000000000,leasedBytes:1000000000},
  {id:"exactIndex",fallbackTier:"metadata",hits:25,misses:75,evictions:2,residentBytes:500000000,targetBytes:500000000,leasedBytes:500000000}
 ]}}};
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:cached}} historical={true} loading={false}/></I18nProvider>);
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));
 expect(screen.getByText('92 %')).toBeVisible();expect(screen.getByText('16 GB')).toBeVisible();
 const data = within(screen.getByRole('row',{name:/Verified Read/}));
 expect(data.getByText('DATA')).toBeVisible();expect(data.getByText('90 %')).toBeVisible();
 expect(data.getByText('4 GB')).toBeVisible();expect(data.getAllByText('6 GB')).toHaveLength(1);
 fireEvent.click(screen.getByRole('checkbox',{name:'Zähler & Reservierung anzeigen'}));
 expect(data.getAllByText('6 GB')).toHaveLength(2);
 expect(within(screen.getByRole('row',{name:/Exact Index/})).getByText('Metadata')).toBeVisible();
 expect(within(screen.getByRole('row',{name:/Historical Proofs/})).getByText('—')).toBeVisible();
 expect(screen.getByRole('progressbar',{name:'Cache-Budget Belegung'})).toHaveAttribute('value','6500000000');
});

it("keeps background verification progress visible across tabs and shows a write-blocking failure", () => {
 const runtime = details.runtime!;
 const scrub = {state:"running",totalContainers:100,verifiedContainers:25,verifiedBytes:1000,readBytes:2000};
 const sample = {...previewSnapshot.telemetry,details:{...details,runtime:{...runtime,scrub}}};
 const view = render(<I18nProvider><DetailTelemetryPanel sample={sample} historical={false} loading={false}/></I18nProvider>);
 expect(screen.getByRole('progressbar',{name:'Hintergrundprüfung'})).toHaveAttribute('value','25');
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));
 expect(screen.getByText(/25 \/ 100/)).toBeVisible();
 view.rerender(<I18nProvider><DetailTelemetryPanel sample={{...sample,details:{...sample.details,runtime:{...runtime,scrub:{...scrub,state:"failed"}}}}} historical={false} loading={false}/></I18nProvider>);
 expect(screen.getByRole('alert')).toHaveTextContent('Neue Schreibzugriffe sind gesperrt');
 expect(screen.queryByRole('progressbar',{name:'Hintergrundprüfung'})).not.toBeInTheDocument();
});

it("distinguishes carried-forward checks, new verification and remaining work", () => {
 const scrub = {state:"running",totalContainers:100,verifiedContainers:75,resumedContainers:60,newlyVerifiedContainers:15,remainingContainers:25,verifiedBytes:1000,readBytes:100};
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:{...details,runtime:{...details.runtime!,scrub}}}} historical={true} loading={false}/></I18nProvider>);
 expect(screen.getByText(/60 aus vorheriger Prüfung übernommen.*15 neu geprüft.*25 noch ausstehend/)).toBeVisible();
 expect(screen.getByRole('progressbar',{name:'Hintergrundprüfung'})).toHaveAttribute('value','75');
});

it("switches cache counters to the five-minute window without changing RAM gauges",()=>{
 const runtime={...details.runtime!,cacheWindow:{seconds:300,pools:[{id:"verifiedRead",hits:2,misses:8,evictions:1}]}};
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:{...details,runtime}}} historical={true} loading={false}/></I18nProvider>);
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));
 expect(screen.getByText('75 %')).toBeVisible();
 fireEvent.click(screen.getByRole('button',{name:'Letzte 5 Minuten'}));
 expect(screen.getByText('20 %')).toBeVisible();
 expect(screen.getByText('1,02 KB')).toBeVisible();
 expect(screen.getByText('Erfasster Zeitraum: 300 s')).toBeVisible();
 fireEvent.click(screen.getByRole('button',{name:'Gesamt seit Mount'}));
 expect(screen.getByText('75 %')).toBeVisible();
});
it("never substitutes lifetime counters for a missing recent window",()=>{
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details}} historical={false} loading={false}/></I18nProvider>);
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));
 fireEvent.click(screen.getByRole('button',{name:'Letzte 5 Minuten'}));
 expect(screen.queryByText('75 %')).not.toBeInTheDocument();
 expect(screen.getByText('Für dieses Zeitfenster sind noch keine Messdaten verfügbar.')).toBeVisible();
});

it("shows cold-read admission evidence and leaves old samples explicitly unavailable",()=>{
 const runtime = {...details.runtime!,reduction:{...details.runtime!.reduction,skippedColdCandidates:931,explorationReads:32,backendBaseReads:140,warmBaseReuses:710,successfulBaseTrials:411}};
 const view=render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:{...details,runtime}}} historical={true} loading={false}/></I18nProvider>);
 fireEvent.click(screen.getByRole('tab',{name:'Lesevermeidung'}));
 expect(within(screen.getByText('Kalte Kandidaten übersprungen').parentElement!).getByText('931')).toBeVisible();
 expect(within(screen.getByText('Backend-Leseversuche (Basen)').parentElement!).getByText('140')).toBeVisible();
 expect(within(screen.getByText('Basen aus RAM wiederverwendet').parentElement!).getByText('710')).toBeVisible();
 view.rerender(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details}} historical={true} loading={false}/></I18nProvider>);
 expect(within(screen.getByText('Kalte Kandidaten übersprungen').parentElement!).getByText('—')).toBeVisible();
});

it("shows the governed Manifest cache and its recent counters",()=>{
 const pool={id:"manifestNodes",fallbackTier:"metadata",hits:90,misses:10,evictions:2,residentBytes:4096,targetBytes:8192,leasedBytes:8192};
 const runtime={...details.runtime!,cacheBudget:{maximumMemoryUsedBasisPoints:9200,effectiveLimitBytes:100000,availableBytes:90000,budgetBytes:90000,pools:[pool]},cacheWindow:{seconds:300,pools:[{id:"manifestNodes",hits:4,misses:1,evictions:0}]}};
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:{...details,runtime}}} historical={false} loading={false}/></I18nProvider>);
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));
 const row=screen.getByText('Manifest Nodes').closest('tr')!;
 expect(within(row).getByText('Metadata')).toBeVisible();
 expect(within(row).getByText('90 %')).toBeVisible();
 fireEvent.click(screen.getByRole('button',{name:'Letzte 5 Minuten'}));
 expect(within(row).getByText('80 %')).toBeVisible();
 expect(within(row).getByText('4,1 KB')).toBeVisible();
});

it("shows the governed Metadata object cache and its recent counters",()=>{
 const pool={id:"metadataObjects",fallbackTier:"metadata",hits:90,misses:10,evictions:2,residentBytes:4096,targetBytes:8192,leasedBytes:8192};
 const runtime={...details.runtime!,cacheBudget:{maximumMemoryUsedBasisPoints:9200,effectiveLimitBytes:100000,availableBytes:90000,budgetBytes:90000,pools:[pool]},cacheWindow:{seconds:300,pools:[{id:"metadataObjects",hits:4,misses:1,evictions:0}]}};
 render(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details:{...details,runtime}}} historical={false} loading={false}/></I18nProvider>);
 fireEvent.click(screen.getByRole('tab',{name:'Caches'}));
 const row=screen.getByText('Metadata Objects').closest('tr')!;
 expect(within(row).getByText('Metadata')).toBeVisible();
 expect(within(row).getByText('90 %')).toBeVisible();
 fireEvent.click(screen.getByRole('button',{name:'Letzte 5 Minuten'}));
 expect(within(row).getByText('80 %')).toBeVisible();
 expect(within(row).getByText('4,1 KB')).toBeVisible();
});

it("shows cache compression gauges at the selected sample and separate lifetime codec costs",()=>{
 const readCacheCompression={decodedResidentBytes:1000000,compressedResidentBytes:2000000,compressedLogicalBytes:8000000,attempts:10,admissions:8,compressionNanos:1000000,hits:40,decompressions:20,decompressionNanos:1000000,promotions:3,demotions:2,failures:0,bypasses:2,workingBytes:0,peakWorkingBytes:100000,maxWorkingBytes:1000000};
 const runtime={...details.runtime!,readCacheCompression};
 const view=render(<I18nProvider><DetailTelemetryPanel initialTab={2} sample={{...previewSnapshot.telemetry,details:{...details,runtime}}} historical={true} loading={false}/></I18nProvider>);
 const panel=within(screen.getByLabelText('Verified Read · RAM-Kompression'));
 expect(panel.getByText('4×')).toBeVisible();
 expect(within(panel.getByText('RAM durch Kompression gespart').parentElement!).getByText('6 MB')).toBeVisible();
 fireEvent.click(screen.getByRole('button',{name:'Letzte 5 Minuten'}));
 expect(panel.getByText('4×')).toBeVisible();
 fireEvent.click(panel.getByText('Kompressionskosten · seit Mount'));
 expect(panel.getByText('50 µs')).toBeVisible();
 expect(panel.getByText('100 µs')).toBeVisible();
 view.rerender(<I18nProvider><DetailTelemetryPanel initialTab={2} sample={{...previewSnapshot.telemetry,details}} historical={true} loading={false}/></I18nProvider>);
 expect(screen.queryByLabelText('Verified Read · RAM-Kompression')).not.toBeInTheDocument();
});


it("separates allocator retention from cache occupancy and preserves missing samples", () => {
 const allocatorMemory = {arenaBytes:9000000000, allocatedBytes:3000000000, freeBytes:6000000000, anonymousResidentBytes:4000000000, trimAttempts:2, lastTrimMicros:108000};
 const sample = {...previewSnapshot.telemetry, details:{...details, runtime:{...details.runtime!, allocatorMemory}}};
 const view = render(<I18nProvider><DetailTelemetryPanel sample={sample} historical={false} loading={false} initialTab={2}/></I18nProvider>);
 fireEvent.click(screen.getByText("Prozessspeicher und Allocator"));
 expect(screen.getByText("Freie Allocator-Blöcke")).toBeVisible();
 expect(screen.getByText("6 GB")).toBeVisible();
 expect(screen.getByText("108 ms")).toBeVisible();
 expect(screen.getByText(/diese Werte werden nicht addiert/)).toBeVisible();
 view.rerender(<I18nProvider><DetailTelemetryPanel sample={{...previewSnapshot.telemetry,details}} historical={true} loading={false} initialTab={2}/></I18nProvider>);
 expect(screen.queryByText("Prozessspeicher und Allocator")).not.toBeInTheDocument();
});
