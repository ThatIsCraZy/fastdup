import {describe, expect, it} from "vitest";
import {diagnose, diagnosticExport, histogramPercentile, historicalChart, type Histogram} from "./history-diagnostics";
import {previewTelemetry, type TelemetrySnapshot} from "./types";

const histogram: Histogram = {upperMicros: [100, 5_000_000, null], counts: [999,1,0], errors: 1};

describe("historical diagnosis evidence", () => {
  it("retains a rare long request even when p99 stays fast", () => {
    expect(histogramPercentile(histogram,99)).toEqual({value:100,overflow:false});
    expect(histogramPercentile(histogram,100)).toEqual({value:5_000_000,overflow:false});
    expect(histogramPercentile({...histogram,counts:[0,0,1]},99)).toEqual({value:5_000_000,overflow:true});
    expect(histogramPercentile({...histogram,counts:[0,0,0]},99)).toBeUndefined();
  });
  it("reports window errors and peaks using their actual interval", () => {
    const sample = {...previewTelemetry, details:{latencyWindow:{from:10,to:11,coveredSeconds:1,read:histogram,write:histogram}}};
    const hints = diagnose(sample);
    expect(hints.map(hint => hint.rule)).toEqual(["latency-read-v1","errors-read-v1","latency-write-v1","errors-write-v1"]);
    expect(hints[0].evidence).toContainEqual({field:"latencyWindow.from",value:10,unit:"unix s"});
    expect(diagnose({...previewTelemetry,details:{latency:{read:{operations:1000,errors:5,p50Micros:100,p95Micros:100,p99Micros:5_000_000},write:{operations:0,errors:0,p50Micros:0,p95Micros:0,p99Micros:0}}}})).toEqual([]);
  });
  it("does not turn a lifetime publication maximum into a simultaneous wait", () => {
    const runtime = {runtimeId:"runtime-a",ioUring:{ringEntries:1,inflightBytes:0,maxInflightBytes:1,peakInflightBytes:0,submitted:0,completed:0},caches:[],
      reduction:{enabled:false,queries:0,candidates:0,acceptedPrefixes:0,acceptedSparseXor:0,savedPayloadBytes:0,fallbacks:0,errors:0},
      pipeline:{operations:[{id:"publicationWait",active:0,completed:1,totalMs:10000,maximumMs:10000,busyMs:10000}],admission:{open:false,reason:"durabilityLag",closures:1,closedMs:100,currentClosedMs:100,maximumClosedMs:100}}};
    const sample = {...previewTelemetry,details:{runtime}};
    expect(diagnose(sample)[0].rule).toBe("admission-closed-v1");
    runtime.pipeline.operations[0].active = 1;
    expect(diagnose(sample)[0].rule).toBe("durability-publication-v1");
    expect(diagnose(sample)[0].evidence).not.toContainEqual(expect.objectContaining({field:"maximumMs"}));
  });
  it("exports selected window, evidence, events and explicit missing coverage", () => {
    const sample: TelemetrySnapshot = {...previewTelemetry,details:{latencyWindow:{from:10,to:11,coveredSeconds:1,read:histogram,write:histogram}}};
    const output = diagnosticExport({from:10,to:20},[sample],sample,null);
    expect(output.window).toEqual({from:10,to:20});
    expect(output.samples).toEqual([sample]);
    expect(output.coverage.eventsAvailable).toBe(false);
    expect(output.coverage.extremaComplete).toBe(false);
    expect(output.findings).toHaveLength(4);
    expect(JSON.parse(JSON.stringify(output)).selected.sequence).toBe(sample.sequence);
  });
  it("does not invent min/max for legacy samples and marks the selected time", () => {
    const chart = historicalChart([previewTelemetry],0,"throughput","en-US");
    expect(chart.xAxis.type).toBe("time");
    expect(chart.series[1].data[0][1]).toBeNull();
    expect(chart.series[0].markLine?.data[0].xAxis).toBe(Date.parse(previewTelemetry.observedAt));
  });
});
