class TaskCreatedError(Exception):
    def __init__(self, messages="Task cannot create in Queue service") -> None:
        self.messages = messages
        super().__init__(self.messages)

    def __str__(self) -> str:
        return self.messages
