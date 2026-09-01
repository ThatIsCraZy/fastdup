const root = document.documentElement;
const buttons = document.querySelectorAll("[data-language]");

function setLanguage(language) {
  root.dataset.language = language;
  root.lang = language;
  document.title = language === "de"
    ? "fastdup · Aus x86-64-Hardware wird eine High-Performance Dedup-Appliance"
    : "fastdup · Turn x86-64 hardware into a high-performance dedup appliance";
  buttons.forEach((button) => {
    button.setAttribute("aria-pressed", String(button.dataset.language === language));
  });
  localStorage.setItem("fastdup-language", language);
}

const preferred = localStorage.getItem("fastdup-language")
  || (navigator.language.toLowerCase().startsWith("de") ? "de" : "en");
setLanguage(preferred);
buttons.forEach((button) => button.addEventListener("click", () => setLanguage(button.dataset.language)));

document.querySelector("[data-copy='install']")?.addEventListener("click", async (event) => {
  const commands = [
    "curl -LO https://github.com/ThatIsCraZy/fastdup/releases/download/v0.5/fastdup-0.5.0-1.el10.x86_64.rpm",
    "sudo dnf install ./fastdup-0.5.0-1.el10.x86_64.rpm",
    "sudo systemctl enable --now fastdup-agent.service fastdup-control.service",
  ].join("\n");
  await navigator.clipboard.writeText(commands);
  const button = event.currentTarget;
  const old = button.innerHTML;
  button.textContent = root.dataset.language === "de" ? "Kopiert" : "Copied";
  window.setTimeout(() => { button.innerHTML = old; }, 1400);
});
