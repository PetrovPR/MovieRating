# Movie Rating Application

## Description

The Movie Rating Application is a data analysis and processing tool designed for working with movie ratings. It is built using Apache Spark and written in Scala. The application allows you to perform the following tasks:

1. **Analyze rating statistics for a specific movie**:
    - Calculates rating statistics for a given movie.
    - Outputs information about the distribution of ratings for the movie and overall statistics for all movies.

2. **Generate movie links by genre**:
    - Creates a CSV file with information about movies that belong to a specified genre, along with their IMDB and TMDB IDs.

## Business Logic

The application performs the following core functions:

1. **Reading Data**:
    - Data is loaded from CSV files containing information on ratings (`ratings`), movies (`movies`), and movie links (`links`).

2. **Calculating Rating Statistics for a Movie**:
    - The `calculateRatingStatisticsForMovie` method in the `RatingService` class takes data on ratings, movies, and movie links, as well as a movie ID.
    - It generates rating distribution statistics for the specified movie and overall statistics for all movies.

3. **Generating a CSV File with Movie Links by Genre**:
    - The `generateMovieLinksCSV` method in the `RatingService` class creates a CSV file containing information about movies of a certain genre along with their IMDB and TMDB IDs.

4. **Saving Results**:
    - Results are saved in CSV and JSON formats.

## Running the Application

1. **Ensure JDK 11 is Installed**:
    - Check this with the command `java -version`. If JDK 11 is not installed, follow the [installation instructions for JDK 11](#).

2. **Build and Run the Application**:
    - Use SBT to build and run the application. In the command line, execute:
      ```sh
      sbt run
      ```
    - Ensure that the configuration files (`application.conf`) are in the correct directory and contain the correct paths to input data and output results.

## Running Tests

1. **Ensure Your Development Environment is Configured to Use JDK 11**:
    - Check the project settings in IntelliJ IDEA and ensure that JDK 11 is used for tests.

2. **Run Tests Using SBT**:
    - To run the tests, use the command:
      ```sh
      sbt test
      ```
    - The tests verify the correctness of the `calculateRatingStatisticsForMovie` and `generateMovieLinksCSV` methods, as well as edge case handling.

## Notes

- For the application to work correctly, the data files (`ratings.csv`, `movies.csv`, `links.csv`) must be in the specified directory, and the configuration in the `application.conf` file must be set up correctly.
- If you encounter issues with class or module access, ensure that all necessary dependencies are installed and the runtime environment is correctly configured.
