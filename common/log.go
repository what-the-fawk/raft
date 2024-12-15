package common

type LogCommand struct {
	ID      string
	Content map[string]string
}

type LogEntry struct {
	Term    int
	Command *LogCommand
}
