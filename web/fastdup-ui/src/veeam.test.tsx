import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { VeeamPage } from "./veeam";
import { emptyApplianceSnapshot } from "./types";
import { I18nProvider } from "./i18n";

afterEach(cleanup);

it("requires an online repository before provisioning", () => {
  render(<I18nProvider><VeeamPage snapshot={emptyApplianceSnapshot()} busy={false} submit={vi.fn()} /></I18nProvider>);
  expect(screen.getByRole("button", { name: "Provisionieren" })).toBeDisabled();
});

it("submits the selected quota and reduction policy through the typed command", () => {
  const submit = vi.fn().mockResolvedValue(undefined);
  const snapshot = { ...emptyApplianceSnapshot(), telemetry: { ...emptyApplianceSnapshot().telemetry, repositoryState: "online" as const } };
  render(<I18nProvider><VeeamPage snapshot={snapshot} busy={false} submit={submit} /></I18nProvider>);
  fireEvent.change(screen.getByLabelText("SSH-Public-Key für die Veeam-Installation"), { target: { value: "ssh-ed25519 test-key" } });
  fireEvent.change(screen.getByLabelText("Temporäres Installationspasswort"), { target: { value: "temporary-password-123456" } });
  fireEvent.click(screen.getByLabelText("Advanced Reduction"));
  fireEvent.click(screen.getByLabelText("Hardened Immutability"));
  fireEvent.click(screen.getByLabelText("Logische Quota setzen"));
  fireEvent.change(screen.getByLabelText("Quota"), { target: { value: "25" } });
  fireEvent.click(screen.getByRole("button", { name: "Provisionieren" }));
  expect(submit).toHaveBeenCalledWith(expect.objectContaining({ kind: "configure_veeam", bootstrap_password: "temporary-password-123456", settings: expect.objectContaining({ hardenedImmutability: true, advancedReduction: "dependent_v1", logicalQuota: { value: 25, unit: "tb" } }) }));
});

it("can close SSH after generating an installation password in the same view", () => {
  const submit = vi.fn().mockResolvedValue(undefined);
  const snapshot = { ...emptyApplianceSnapshot(), telemetry: { ...emptyApplianceSnapshot().telemetry, repositoryState: "online" as const } };
  render(<I18nProvider><VeeamPage snapshot={snapshot} busy={false} submit={submit} /></I18nProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Passwort erzeugen" }));
  fireEvent.click(screen.getByLabelText("SSH-Installationszugang aktiv"));
  fireEvent.click(screen.getByRole("button", { name: "Provisionieren" }));
  expect(submit).toHaveBeenCalledWith(expect.objectContaining({ settings: expect.objectContaining({ sshEnabled: false }) }));
  expect(submit.mock.calls[0][0]).not.toHaveProperty("bootstrap_password");
});
