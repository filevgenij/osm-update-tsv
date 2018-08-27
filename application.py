from cleo import Application
from app.command.AppendInfoCommand import AppendInfoCommand
from app.command.PublisherCommand import PublisherCommand
from app.command.WorkerCommand import WorkerCommand

application = Application()

application.add(AppendInfoCommand())
application.add(PublisherCommand())
application.add(WorkerCommand())
if __name__ == '__main__':
    application.run()
