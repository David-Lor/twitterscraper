from twitterscraper.services.persistence import Repository


def db_migrate():
    Repository.run_migrations()


def db_generate_migration(name: str):
    Repository.generate_migration(name)
