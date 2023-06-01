from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWorstAndBestMoviePerGenre(MRJob):

    def mapper_movie_average(self, _, line):
        user, movie, rating, genre, date = line.split(',')
        data = {}
        data["genre"] = genre
        data["rating"] = float(rating)
        yield movie, data
    
    def reducer_movie_average(self, movie, gen):
        data = list(gen)
        rating_sum = 0
        counter = 0
        genre = data[0]["genre"]
        for row in data:
            rating_sum += row["rating"]
            counter += 1
        l = {}
        l["averageScore"] = rating_sum / counter if counter != 0 else 0
        l["movie"] = movie
        yield genre, l

    def reducer_worst_best_movie(self, genre, gen):
        data = list(gen)

        worst_average_movie = data[0]["averageScore"]
        worst_movie = data[0]["movie"]
        best_average_movie = data[0]["averageScore"]
        best_movie = data[0]["movie"]

        for movie in data:
            if movie["averageScore"] > best_average_movie:
                best_average_movie = movie["averageScore"]
                best_movie = movie["movie"]
            elif movie["averageScore"] < worst_average_movie:
                worst_average_movie = movie["averageScore"]
                worst_movie = movie["movie"]
        
        l = {}
        l["worstMovie"] = worst_movie
        l["worstMovieAverageScore"] = worst_average_movie
        l["BestMovie"] = best_movie
        l["BestMovieAverageScore"] = best_average_movie

        yield genre, l

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movie_average,
                   reducer=self.reducer_movie_average),
            MRStep(reducer=self.reducer_worst_best_movie)
        ]

if __name__ == '__main__':
    MRWorstAndBestMoviePerGenre.run()