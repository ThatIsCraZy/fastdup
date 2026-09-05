import { expect, it } from "vitest";
import { translate } from "./i18n";
it("translates messages with interpolation while preserving names and technical terms", () => {
  expect(translate("en", "Neue Freigabe")).toBe("New share");
  expect(translate("de", "Neue Freigabe")).toBe("Neue Freigabe");
  expect(translate("en", "Freigabe „{name}“ löschen? Aktive Sessions werden getrennt.", {name:"Meine-Daten"})).toBe("Delete share “Meine-Daten”? Active sessions will be disconnected.");
  expect(translate("de", "Repository")).toBe("Repository");
});
