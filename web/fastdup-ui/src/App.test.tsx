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
