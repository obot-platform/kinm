package otel

import "os"

type Level string

const (
	LevelOff     Level = "off"
	LevelBasic   Level = "basic"
	LevelVerbose Level = "verbose"

	DefaultLevel = LevelOff
)

func ParseLevel(v string) Level {
	return Level(v).normalized()
}

func CurrentLevel() Level {
	return ParseLevel(os.Getenv("KINM_TRACE_LEVEL"))
}

func (l Level) normalized() Level {
	switch l {
	case LevelOff, LevelBasic, LevelVerbose:
		return l
	default:
		return DefaultLevel
	}
}

func (l Level) Enabled(min Level) bool {
	return levelRank(l.normalized()) >= levelRank(min.normalized())
}

func levelRank(l Level) int {
	switch l {
	case LevelVerbose:
		return 2
	case LevelBasic:
		return 1
	default:
		return 0
	}
}
