package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func main() {
	url := "http://127.0.0.1/core/loginguest?userid=bpictpw&password=Pointwest!2345678"
	method := "POST"
	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))

	cookies := res.Cookies()
	for _, cookie := range cookies {
		fmt.Println("Cookie Name: ", cookie.Name)
		fmt.Println("Cookie Value: ", cookie.Value)
	}
}
