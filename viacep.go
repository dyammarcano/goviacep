package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type ServiceUrl string

const (
	ViaCEPUrl   ServiceUrl = "https://viacep.com.br/ws/%s/json"
	CorreiosUrl ServiceUrl = "https://buscacepinter.correios.com.br/app/endereco/carrega-cep-endereco.php?pagina=1/app/endereco/index.php&cepaux=&mensagem_alerta=&endereco=%s&tipoCEP=ALL"
	BuscaCEPUrl ServiceUrl = "https://buscacep.com.br/pesquisa?q=%s"
)

const (
	createTableViaCEP = `CREATE TABLE IF NOT EXISTS viacep (id INTEGER PRIMARY KEY AUTOINCREMENT,cep TEXT NOT NULL,logradouro TEXT NOT NULL,complemento TEXT NOT NULL,bairro TEXT NOT NULL,localidade TEXT NOT NULL,uf TEXT NOT NULL,ibge TEXT NOT NULL,gia TEXT NOT NULL,ddd TEXT NOT NULL,siafi TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,valid BOOLEAN NOT NULL DEFAULT 0);`
	insertViaCEP      = `INSERT INTO viacep (cep,logradouro,complemento,bairro,localidade,uf,ibge,gia,ddd,siafi,created_at,updated_at,valid) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%v');`
	selectViaCEP      = `SELECT * FROM viacep;`
	updateViaCEP      = `UPDATE viacep SET logradouro = '%s',complemento = '%s',bairro = '%s',localidade = '%s',uf = '%s',ibge = '%s',gia = '%s',ddd = '%s',siafi = '%s',updated_at = '%s' WHERE cep = '%s';`
	deleteViaCEP      = `DELETE FROM viacep WHERE cep = '%s';`
	getLastViaCEP     = `SELECT * FROM viacep WHERE valid = 'false' ORDER BY id DESC LIMIT 1;`
)

type Searcher struct {
	Url string
}

func (s *Searcher) SetUrl(url ServiceUrl) string {
	return s.Url
}

type BuscaCEPInter struct {
	Erro     bool    `json:"erro"`
	Mensagem string  `json:"mensagem"`
	Total    int     `json:"total"`
	Dados    []Dados `json:"dados"`
}

//{
//    "erro": false,
//    "mensagem": "DADOS ENCONTRADOS COM SUCESSO.",
//    "total": 1,
//    "dados": [
//        {
//            "uf": "SP",
//            "localidade": "S\u00e3o Paulo",
//            "locNoSem": "",
//            "locNu": "",
//            "localidadeSubordinada": "",
//            "logradouroDNEC": "Pra\u00e7a da S\u00e9 - lado \u00edmpar",
//            "logradouroTextoAdicional": "",
//            "logradouroTexto": "",
//            "bairro": "S\u00e9",
//            "baiNu": "",
//            "nomeUnidade": "",
//            "cep": "01001000",
//            "tipoCep": "2",
//            "numeroLocalidade": "",
//            "situacao": "",
//            "faixasCaixaPostal": [],
//            "faixasCep": []
//        }
//    ]
//}

type Dados struct {
	Uf                       string `json:"uf"`
	Localidade               string `json:"localidade"`
	LocNoSem                 string `json:"locNoSem"`
	LocNu                    string `json:"locNu"`
	LocalidadeSubordinada    string `json:"localidadeSubordinada"`
	LogradouroDNEC           string `json:"logradouroDNEC"`
	LogradouroTextoAdicional string `json:"logradouroTextoAdicional"`
	LogradouroTexto          string `json:"logradouroTexto"`
	Bairro                   string `json:"bairro"`
	BaiNu                    string `json:"baiNu"`
	NomeUnidade              string `json:"nomeUnidade"`
	Cep                      string `json:"cep"`
	TipoCep                  string `json:"tipoCep"`
	NumeroLocalidade         string `json:"numeroLocalidade"`
	Situacao                 string `json:"situacao"`
	FaixasCaixaPostal        []any  `json:"faixasCaixaPostal"`
	FaixasCep                []any  `json:"faixasCep"`
}

type ViaCEP struct {
	Id          int    `json:"id" db:"id"`
	Cep         string `json:"cep" db:"cep"`
	Logradouro  string `json:"logradouro" db:"logradouro"`
	Complemento string `json:"complemento" db:"complemento"`
	Bairro      string `json:"bairro" db:"bairro"`
	Localidade  string `json:"localidade" db:"localidade"`
	Uf          string `json:"uf" db:"uf"`
	Ibge        string `json:"ibge" db:"ibge"`
	Gia         string `json:"gia" db:"gia"`
	Ddd         string `json:"ddd" db:"ddd"`
	Siafi       string `json:"siafi" db:"siafi"`
	CreatedAt   string `json:"created_at" db:"created_at"`
	UpdatedAt   string `json:"updated_at" db:"updated_at"`
	Error       bool   `json:"erro"`
	Valid       bool   `json:"valid" db:"valid"`
}

func requestViaCEP(cep string) (*ViaCEP, error) {
	req, err := http.Get(fmt.Sprintf(string(ViaCEPUrl), cep))
	if err != nil {
		return nil, err
	}
	defer req.Body.Close()

	viaCEP := &ViaCEP{}
	if err = json.NewDecoder(req.Body).Decode(viaCEP); err != nil {
		return nil, err
	}

	viaCEP.Cep = formatCep(cep)
	viaCEP.CreatedAt = time.Now().Format("2006-01-02 15:04:05")
	viaCEP.UpdatedAt = viaCEP.CreatedAt

	return viaCEP, nil
}

func (v *ViaCEP) add() string {
	return fmt.Sprintf(insertViaCEP, v.Cep, v.Logradouro, v.Complemento, v.Bairro, v.Localidade, v.Uf, v.Ibge, v.Gia, v.Ddd, v.Siafi, v.CreatedAt, v.UpdatedAt, checkAllFields(v))
}

func (v *ViaCEP) update() string {
	return fmt.Sprintf(updateViaCEP, v.Logradouro, v.Complemento, v.Bairro, v.Localidade, v.Uf, v.Ibge, v.Gia, v.Ddd, v.Siafi, v.UpdatedAt, v.Cep)
}

func (v *ViaCEP) delete() string {
	return fmt.Sprintf(deleteViaCEP, v.Cep)
}

func formatCep(cep string) string {
	return fmt.Sprintf("%s-%s", cep[:5], cep[5:])
}

func checkAllFields(v *ViaCEP) bool {
	return v.Cep != "" && v.Logradouro != "" && v.Bairro != "" && v.Localidade != "" && v.Uf != "" && v.Ibge != "" && v.Gia != "" && v.Ddd != "" && v.Siafi != ""
}
