import {cleanup, render, screen} from "@testing-library/react";
import {afterEach, expect, it} from "vitest";
import {StorageOverview} from "./storage-overview";
import {I18nProvider} from "./i18n";
import {previewSnapshot} from "./types";
afterEach(cleanup);
it("shows logical versus combined physical allocation and the bound storage roles",()=>{
 const snapshot={...previewSnapshot,telemetry:{...previewSnapshot.telemetry,storageUsage:{logicalAllocatedBytes:1e12,logicalObservedAt:1788750000,metadataUsedBytes:1e9,metadataCapacityBytes:10e9,dataUsedBytes:99e9,dataCapacityBytes:1e12}}};
 render(<I18nProvider><StorageOverview snapshot={snapshot}/></I18nProvider>);
 expect(screen.getByRole('heading',{name:'Laufwerke & Belegung'})).toBeVisible();
 expect(screen.getByText('100 GB')).toBeVisible();
 expect(screen.getAllByText('1 TB').length).toBeGreaterThan(0);
 expect(screen.getByText('Metadaten, Indizes und Small Files')).toBeVisible();
 expect(screen.getByRole('progressbar',{name:'Metadata Belegung'})).toHaveAttribute('value','1000000000');
 expect(screen.queryByText('Metadata-Target auswählen')).not.toBeInTheDocument();
});
it("keeps unavailable usage distinct from an empty repository even without inventory",()=>{
 render(<I18nProvider><StorageOverview snapshot={{...previewSnapshot,targets:[],telemetry:{...previewSnapshot.telemetry,storageUsage:undefined}}}/></I18nProvider>);
 expect(screen.getAllByText('—').length).toBeGreaterThan(0);
 expect(screen.queryByText('0 B')).not.toBeInTheDocument();
 expect(screen.getByText('Die logische Belegung wird von der laufenden Runtime ermittelt.')).toBeVisible();
});
