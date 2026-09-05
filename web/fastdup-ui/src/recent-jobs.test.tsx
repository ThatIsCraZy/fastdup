import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { RecentJobs } from "./recent-jobs";
import { I18nProvider } from "./i18n";
import type { JobStatus } from "./types";
afterEach(cleanup);
it("keeps the tray expanded across live job updates and shows failures", () => {
 const job: JobStatus={id:'job',kind:'mount',state:'running',progressBasisPoints:2000,createdAt:100,updatedAt:101,message:'Mounting'};
 const view=render(<I18nProvider><RecentJobs jobs={[job]}/></I18nProvider>);
 const toggle=screen.getByRole('button',{name:/Letzte Jobs/});expect(toggle).toHaveAttribute('aria-expanded','false');
 fireEvent.click(toggle);expect(screen.getByRole('progressbar')).toHaveValue(2000);
 const resize=screen.getByRole('separator',{name:'Höhe der Jobliste'});
 expect(resize).toHaveAttribute('aria-valuenow','150');
 fireEvent.keyDown(resize,{key:'ArrowUp'});expect(resize).toHaveAttribute('aria-valuenow','174');
 fireEvent.keyDown(resize,{key:'Home'});expect(resize).toHaveAttribute('aria-valuenow','96');
 view.rerender(<I18nProvider><RecentJobs jobs={[{...job,state:'failed',message:'Mount failed',updatedAt:102}]}/></I18nProvider>);
 expect(toggle).toHaveAttribute('aria-expanded','true');expect(screen.getByText('Mount failed')).toBeVisible();expect(within(screen.getByRole('table')).getByText('Fehlgeschlagen')).toBeVisible();
 fireEvent.click(toggle);expect(screen.queryByRole('table')).not.toBeInTheDocument();
});
