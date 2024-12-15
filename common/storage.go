package common

import (
	"fmt"
	"reflect"
	"time"
)

type Data struct {
	ID      string
	Content map[string]string
}

type Storage struct {
	resources map[string]*Data
}

func NewStorage() *Storage {
	return &Storage{
		resources: make(map[string]*Data),
	}
}

func (s *Storage) Apply(cmd *LogCommand) string {
	if cmd.Content == nil {
		delete(s.resources, cmd.ID)
	} else {
		s.resources[cmd.ID] = &Data{
			ID:      cmd.ID,
			Content: cmd.Content,
		}
	}

	return cmd.ID
}

func (s *Storage) Get(key string) (*Data, bool) {
	r, ok := s.resources[key]
	if !ok {
		return nil, false
	}
	copy := &Data{
		ID:      r.ID,
		Content: make(map[string]string, len(r.Content)),
	}
	for k, v := range r.Content {
		copy.Content[k] = v
	}
	return copy, true
}

func (s *Storage) GetCreateCommand(content map[string]string) *LogCommand {
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	return &LogCommand{
		ID:      id,
		Content: content,
	}
}

func (s *Storage) GetPutCommand(id string, content map[string]string) *LogCommand {
	return &LogCommand{
		ID:      id,
		Content: content,
	}
}

func (s *Storage) GetPatchCommand(id string, expectedContent map[string]string, newContent map[string]string) *LogCommand {
	if resource, ok := s.resources[id]; ok {
		if !reflect.DeepEqual(resource.Content, expectedContent) {
			return nil
		}
	} else {
		return nil
	}

	return &LogCommand{
		ID:      id,
		Content: newContent,
	}
}

func (s *Storage) GetDeleteCommand(id string) *LogCommand {
	return &LogCommand{
		ID: id,
	}
}
