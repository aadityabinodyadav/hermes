package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
)

func main() {
	// regex to match /* ... */
	re := regexp.MustCompile(`(?s)/\*\*?.*?\*/\n*`)

	err := filepath.Walk("pkg", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".go" {
			content, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}
			newContent := re.ReplaceAll(content, []byte(""))
			if len(newContent) != len(content) {
				ioutil.WriteFile(path, newContent, 0644)
				fmt.Printf("Stripped block comments from %s\n", path)
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
}
