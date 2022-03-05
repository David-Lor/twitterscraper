# Twitter AMQP-based Scraper

Twitter profile scraping platform based on microservices and job queues, using AMQP and [Nitter](https://github.com/zedeus/nitter). The platform can:

- Detect and persist old and new tweets from configured profiles
- Detect deleted tweets
- ~~Archive tweet snapshots on web.archive.org~~
- ~~Trigger AMQP "events" when a tweet is detected or deleted~~

This project uses:

- async Python code
- Twitter API & [Nitter](https://github.com/zedeus/nitter) scraping (beautifulsoup4)
- [typer](https://github.com/tiangolo/typer) as CLI interface
- [pydantic](https://github.com/samuelcolvin/pydantic) for models and settings schema
- [aio_pika](https://github.com/mosquito/aio-pika) as AMQP client
- [sqlalchemy](https://www.sqlalchemy.org) + [sqlmodel](https://github.com/tiangolo/sqlmodel) as ORM (using Postgres as database); [alembic](https://github.com/sqlalchemy/alembic) for DB migrations

(~~strikethrough~~ lines are features not currently available, that will be implemented in the future)

**This project is currently experimental, incomplete and under development.**

## Components

- Jobs: AMQP messages representing a job that must be completed by a Worker Component.
  - FetchAndPersist: fetch all the tweets from a certain profile, during a certain time range, and persist its data in DB. ~~Each found tweet triggers an Archive.org task and a TweetDetected event~~.
  - ~~Archive.org: save a snapshot of the tweet on web.archive.org, and persist the snapshot URL on the tweet row in DB.~~
  - PersistedReview: review tweets for validating if they still exist on Twitter. If not, persists 'now' as the deletion-detection timestamp on the tweet row in DB. ~~Triggers a TweetDeleted event.~~
- ~~Events: AMQP messages representing an event that can be consumed by other services.~~
  - ~~TweetDetected: triggered when we detect a new message~~
  - ~~TweetDeleted: triggered when we detect that a certain tweet has been deleted.~~
- Tasks: periodically-triggered actions that create and enqueue Jobs.
  - SyncProfilesTweets: for each active profile on the platform, scan if the profile still exists (if suspended, deleted or privated, stop tracking it by setting the corresponding flag on database). Then, for each persisted tweet from active profiles, create a PersistedReview job.
  - NewTweetsScan: for each active profile on the platform, create a FetchAndPersist job between the last 'scan-timestamp' and 'now' (if the time span is too long, may be split in several jobs). Persist 'now' as the 'scan-timestamp' on the profile row in DB. This task should run after SyncProfilesTweets, so the profiles are updated beforehand.
- Components: the application must run "multiple times in different execution modes", a.k.a. different services, with a different task at hand.
  - Creator: called on-demand, when we want to add a new profile to the platform. Scrapes basic profile info, persist it and create
  - Tasks: called periodically (Cron) or on-demand. Perform certain processings, then create new jobs. One for each type of Task.
  - Workers: run constantly for processing incoming Jobs. One for each type of Job.

The services Scheduler and all the Workers must be deployed for the platform to work. The Creator component is called on-demand.
