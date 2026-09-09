package main

import (
	"net/http"
	"os"
)

func (a *applicationDependencies) uiConsoleHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		a.notFoundResponse(w, r)
		return
	}

	htmlContent, err := os.ReadFile("ui/index.html")
	if err != nil {
		a.serverErrorResponse(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(htmlContent)
}
