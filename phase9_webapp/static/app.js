const uploadForm = document.getElementById("upload-form");
const fileInput = document.getElementById("csv-file");
const uploadMessage = document.getElementById("upload-message");
const dashboardPanel = document.getElementById("dashboard-panel");
const cityControls = document.getElementById("city-controls");
const citySelect = document.getElementById("city-select");
const summary = document.getElementById("summary");
const categoryButtons = document.querySelectorAll(".category-btn");

let cities = [];
let selectedCategory = null;

uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  const file = fileInput.files[0];
  if (!file) {
    uploadMessage.textContent = "Select a CSV file first.";
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  uploadMessage.textContent = "Uploading...";
  const response = await fetch("/api/upload", {
    method: "POST",
    body: formData,
  });

  const result = await response.json();
  if (!result.ok) {
    uploadMessage.textContent = result.error || "Upload failed.";
    dashboardPanel.classList.add("hidden");
    return;
  }

  cities = result.cities || [];
  uploadMessage.textContent = `Upload successful. Loaded ${result.rows} rows.`;
  dashboardPanel.classList.remove("hidden");
  cityControls.classList.add("hidden");
  citySelect.innerHTML = "";
  summary.textContent = "Choose one of the three buttons to start analysis.";
  selectedCategory = null;
  categoryButtons.forEach((button) => button.classList.remove("active"));
  Plotly.purge("chart");
});

categoryButtons.forEach((button) => {
  button.addEventListener("click", () => {
    if (!cities.length) {
      summary.textContent = "Upload CSV data first.";
      return;
    }

    selectedCategory = button.dataset.category;
    categoryButtons.forEach((btn) => btn.classList.remove("active"));
    button.classList.add("active");

    loadCityDropdown();
    loadAnalysis();
  });
});

citySelect.addEventListener("change", () => {
  if (selectedCategory) {
    loadAnalysis();
  }
});

function loadCityDropdown() {
  citySelect.innerHTML = "";
  cities.forEach((city) => {
    const option = document.createElement("option");
    option.value = city;
    option.textContent = city;
    citySelect.appendChild(option);
  });
  cityControls.classList.remove("hidden");
}

async function loadAnalysis() {
  const city = citySelect.value;
  if (!city || !selectedCategory) {
    return;
  }

  const query = new URLSearchParams({ category: selectedCategory, city });
  const response = await fetch(`/api/analysis?${query.toString()}`);
  const result = await response.json();

  if (!result.ok) {
    summary.textContent = result.error || "Could not load analysis.";
    return;
  }

  Plotly.react("chart", result.chart.data, result.chart.layout, { responsive: true });
  summary.textContent = result.summary;
}
