from handlers import RatingHandler, ActorHandler, CountAvgHandler, ComputeTopHandler
actor_avg_ratings_dir = "../data/_actor_avg_ratings"
actor_dir = "../data/_actors"
ratings_dir = "../data/_ratings"

source_name_basics = '../data/name.basics.tsv'
source_title_akas = '../data/title.akas.tsv'
source_title_principals = '../data/title.principals.tsv'
source_title_ratings = '../data/title.ratings.tsv'

RATINGS_SHARDS: int = 300
ACTORS_SHARDS = 10
NAMES_SHARDS = 20
NUM_TOP = 10

rating_handler = RatingHandler(source_title_ratings, ratings_dir, RATINGS_SHARDS)
actor_handler = ActorHandler(source_title_principals, actor_dir, ACTORS_SHARDS)
avg_handler = CountAvgHandler(actor_dir, actor_avg_ratings_dir)
top_handler = ComputeTopHandler(actor_avg_ratings_dir, source_name_basics, NUM_TOP)

rating_handler.run()
actor_handler.run(rating_handler=rating_handler)
avg_handler.run_multi()
top_handler.run()

top_handler.print_top()
