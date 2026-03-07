package main

import (
	"embed"
	"html/template"
	"log"
	"net/http"
)

//go:embed pages/*
var pages embed.FS
var templateFile *template.Template

func setupPages() {
	var err error
	templateFile, err = template.ParseFS(pages, "pages/*")
	if err != nil {
		log.Fatalln(err)
	}
}

type BasePage struct {
	Title    string
	SubPages []string
}

func getStartPage(writer http.ResponseWriter, _ *http.Request) {
	var err error
	page := BasePage{Title: "Overview"}
	page.SubPages, err = getCategoryNames()
	if err != nil {
		log.Println(err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = templateFile.ExecuteTemplate(writer, "StartPage", page)
	if err != nil {
		log.Println(err)
		writer.WriteHeader(http.StatusInternalServerError)
	}
}
