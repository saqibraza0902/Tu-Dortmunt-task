const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const fs = require("fs");
const math = require("mathjs");
const ss = require("simple-statistics");

const disneyPlusData = [];
const netflixData = [];

function cleanAgeValue(age) {
  if (!age || age.trim() === "") return null; // Empty cells
  if (age.trim().toLowerCase() === "all") return 0; // Treat "all" as age 0
  if (age.includes("+")) return parseInt(age.replace("+", "")); // Strip the "+" and convert to integer
  return parseInt(age); // Convert to integer if it's just a number
}

// Helper function to clean Rotten Tomatoes score
function cleanScore(score) {
  if (!score || !score.includes("/")) return null;
  return parseFloat(score.split("/")[0]); // Extract the score (e.g., 80 from "80/100")
}

// Read the CSV file
fs.createReadStream("./data.csv")
  .pipe(csv())
  .on("data", (row) => {
    // Clean age value for the row
    const cleanedAge = cleanAgeValue(row.Age);

    // Clean Rotten Tomatoes score for the row
    const cleanedScore = cleanScore(row["Rotten Tomatoes"]);

    // Check if the movie is available on Netflix or Disney+ and age and score are valid
    if (
      parseInt(row.Netflix) === 1 &&
      cleanedAge !== null &&
      cleanedScore !== null
    ) {
      netflixData.push({
        age_restriction: cleanedAge,
        score: cleanedScore,
      });
    }
    if (
      parseInt(row["Disney+"]) === 1 &&
      cleanedAge !== null &&
      cleanedScore !== null
    ) {
      disneyPlusData.push({
        age_restriction: cleanedAge,
        score: cleanedScore,
      });
    }
  })
  .on("end", () => {
    console.log("CSV file successfully processed");
    performDescriptiveAnalysis();
    performHypothesisTests();
  });

// Function to perform descriptive analysis
function performDescriptiveAnalysis() {
  // Calculate descriptive statistics for age restrictions and Rotten Tomatoes scores
  const disneyAges = disneyPlusData.map((movie) => movie.age_restriction);
  const netflixAges = netflixData.map((movie) => movie.age_restriction);
  const disneyScores = disneyPlusData.map((movie) => movie.score);
  const netflixScores = netflixData.map((movie) => movie.score);

  console.log("Descriptive Statistics:");

  // Age restrictions: mean, median, std dev
  console.log(
    "Disney+ Age Restriction: Mean =",
    math.mean(disneyAges),
    "Median =",
    math.median(disneyAges),
    "StdDev =",
    math.std(disneyAges)
  );

  console.log(
    "Netflix Age Restriction: Mean =",
    math.mean(netflixAges),
    "Median =",
    math.median(netflixAges),
    "StdDev =",
    math.std(netflixAges)
  );

  // Rotten Tomatoes scores: mean, median, std dev
  console.log(
    "Disney+ Rotten Tomatoes Score: Mean =",
    math.mean(disneyScores),
    "Median =",
    math.median(disneyScores),
    "StdDev =",
    math.std(disneyScores)
  );

  console.log(
    "Netflix Rotten Tomatoes Score: Mean =",
    math.mean(netflixScores),
    "Median =",
    math.median(netflixScores),
    "StdDev =",
    math.std(netflixScores)
  );
  exportRottenTomatoesScores();
}

// Function to perform hypothesis tests
function performHypothesisTests() {
  const disneyAges = disneyPlusData.map((movie) => movie.age_restriction);
  const netflixAges = netflixData.map((movie) => movie.age_restriction);
  const disneyScores = disneyPlusData.map((movie) => movie.score);
  const netflixScores = netflixData.map((movie) => movie.score);

  // Hypothesis test: Is age restriction lower on Disney+?
  const ageTTest = ss.tTestTwoSample(disneyAges, netflixAges);
  console.log("T-test for Age Restriction (Disney+ vs Netflix):", ageTTest);

  // Hypothesis test: Is there a difference in Rotten Tomatoes scores?
  const scoreTTest = ss.tTestTwoSample(disneyScores, netflixScores);
  console.log(
    "T-test for Rotten Tomatoes Scores (Disney+ vs Netflix):",
    scoreTTest
  );
}

function exportRottenTomatoesScores() {
  // Collect scores from both platforms
  const disneyScores = disneyPlusData.map((movie) => movie.score);
  const netflixScores = netflixData.map((movie) => movie.score);

  // Prepare data for CSV export (each movie score as a row)
  const scoresData = [];
  const maxLength = Math.max(disneyScores.length, netflixScores.length);

  for (let i = 0; i < maxLength; i++) {
    scoresData.push({
      "Movie Number": i + 1,
      "Disney+ Score": disneyScores[i] || "", // If there is no score, leave it empty
      "Netflix Score": netflixScores[i] || "", // If there is no score, leave it empty
    });
  }

  // CSV writer configuration
  const csvWriter = createCsvWriter({
    path: "./platform_scores.csv", // Specify the output CSV file path
    header: [
      { id: "Movie Number", title: "Movie Number" },
      { id: "Disney+ Score", title: "Disney+ Score" },
      { id: "Netflix Score", title: "Netflix Score" },
    ],
  });

  // Write data to CSV
  csvWriter
    .writeRecords(scoresData)
    .then(() => {
      console.log("Scores successfully written to platform_scores.csv");
    })
    .catch((err) => {
      console.error("Error writing scores to CSV:", err);
    });
}
