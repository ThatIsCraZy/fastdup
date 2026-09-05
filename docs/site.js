const root = document.documentElement;
const buttons = document.querySelectorAll("[data-language]");

function setLanguage(language) {
  const selected = language === "en" ? "en" : "de";
  root.dataset.language = selected;
  root.lang = selected;
  document.title =
    selected === "de"
      ? "fastdup · Weniger Speicher. Dieselben Dateien. In Rust."
      : "fastdup · Less storage. The same files. Built with Rust.";
  buttons.forEach((button) => {
    button.setAttribute(
      "aria-pressed",
      String(button.dataset.language === selected),
    );
  });
  try {
    localStorage.setItem("fastdup-language", selected);
  } catch {
    // Language switching also works when browser storage is unavailable.
  }
}

let preferred = navigator.language.toLowerCase().startsWith("de") ? "de" : "en";
try {
  const saved = localStorage.getItem("fastdup-language");
  if (saved === "de" || saved === "en") preferred = saved;
} catch {
  // Use the browser language when preferences cannot be read.
}
setLanguage(preferred);
buttons.forEach((button) =>
  button.addEventListener("click", () => setLanguage(button.dataset.language)),
);

document
  .querySelector("[data-copy='install']")
  ?.addEventListener("click", async () => {
    const status = document.querySelector("#copy-status");
    try {
      await navigator.clipboard.writeText(
        document.querySelector("#install code").textContent.trim(),
      );
      status.textContent =
        root.lang === "de" ? "Befehle kopiert." : "Commands copied.";
    } catch {
      status.textContent =
        root.lang === "de"
          ? "Kopieren nicht verfügbar. Bitte die Befehle markieren und manuell kopieren."
          : "Clipboard unavailable. Please select and copy the commands manually.";
    }
  });
