package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/360EntSecGroup-Skylar/excelize"
)

type Cell struct {
	bin                  int
	rnn                  int
	taxpayerOrganization int
	taxpayerName         int
	ownerName            int
	ownerIin             int
	ownerRnn             int
	inspectionActNo      int
	inspectionDate       int
}

type WrongAddress struct {
	bin                  string
	rnn                  string
	taxpayerOrganization string
	taxpayerName         string
	ownerName            string
	ownerIin             string
	ownerRnn             string
	inspectionActNo      string
	inspectionDate       string
}

func (p WrongAddress) toString() string {
	var id string

	if p.bin != "" {
		id = "\"_id\": \"" + p.bin + "\""
	}
	return "{ \"index\": {" + id + "}} \n" +
		"{ \"bin\":\"" + p.bin + "\"" +
		", \"rnn\":\"" + p.rnn + "\"" +
		", \"taxpayer_organization\":\"" + p.taxpayerOrganization + "\"" +
		", \"taxpayer_name\":\"" + p.taxpayerName + "\"" +
		", \"owner_name\":\"" + p.ownerName + "\"" +
		", \"owner_iin\":\"" + p.ownerIin + "\"" +
		", \"owner_rnn\":\"" + p.ownerRnn + "\"" +
		", \"inspection_act_no\":\"" + p.inspectionActNo + "\"" +
		", \"inspection_date\":\"" + p.inspectionDate + "\"" +
		"}\n"
}

func parseAndSendToES(TaxInfoDescription string, f *excelize.File) error {
	cell := Cell{1, 2, 3, 4, 5,
		6, 7, 8, 9}

	replacer := strings.NewReplacer(
		"\"", "'",
		"\\", "/",
		"\n", "",
		"\n\n", "",
		"\r", "")

	for _, name := range f.GetSheetMap() {
		// Get all the rows in the name
		rows := f.GetRows(name)
		var input strings.Builder
		for i, row := range rows {
			if i < 3 {
				continue
			}
			wrongAddress := new(WrongAddress)
			for j, colCell := range row {
				switch j {
				case cell.bin:
					wrongAddress.bin = replacer.Replace(colCell)
				case cell.rnn:
					wrongAddress.rnn = replacer.Replace(colCell)
				case cell.taxpayerOrganization:
					wrongAddress.taxpayerOrganization = replacer.Replace(colCell)
				case cell.taxpayerName:
					wrongAddress.taxpayerName = replacer.Replace(colCell)
				case cell.ownerName:
					wrongAddress.ownerName = replacer.Replace(colCell)
				case cell.ownerIin:
					wrongAddress.ownerIin = replacer.Replace(colCell)
				case cell.ownerRnn:
					wrongAddress.ownerRnn = replacer.Replace(colCell)
				case cell.inspectionActNo:
					wrongAddress.inspectionActNo = replacer.Replace(colCell)
				case cell.inspectionDate:
					wrongAddress.inspectionDate = replacer.Replace(colCell)

				}
			}
			if wrongAddress.bin != "" {
				input.WriteString(wrongAddress.toString())
			}
			if i%20000 == 0 {
				if errorT := sendPost(TaxInfoDescription, input.String()); errorT != nil {
					return errorT
				}
				input.Reset()
			}
		}
		if input.Len() != 0 {
			if errorT := sendPost(TaxInfoDescription, input.String()); errorT != nil {
				return errorT
			}
		}
	}
	return nil
}

func sendPost(TaxInfoDescription string, query string) error {
	data := []byte(query)
	r := bytes.NewReader(data)
	resp, err := http.Post("http://locahost:9200/wrong_address/companies/_bulk", "application/json", r)
	if err != nil {
		fmt.Println("Could not send the data to elastic search " + TaxInfoDescription)
		fmt.Println(err)
		return err
	}
	fmt.Println(TaxInfoDescription + " " + resp.Status)
	return nil
}
