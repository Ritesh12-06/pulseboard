import multiprocessing
import hackernews_producer
import github_producer
import reddit_producer

if __name__ == "__main__":
    processes = [
        multiprocessing.Process(target=hackernews_producer.run, name="HackerNews"),
        multiprocessing.Process(target=github_producer.run, name="GitHub"),
        multiprocessing.Process(target=reddit_producer.run, name="Reddit"),
    ]

    print("Starting all producers...")
    for p in processes:
        p.start()

    for p in processes:
        p.join()