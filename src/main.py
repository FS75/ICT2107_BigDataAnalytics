import asyncio

from scrape import scrape_glassdoor


def main():
    asyncio.run(scrape_glassdoor())


if __name__ == "__main__":
    main()
