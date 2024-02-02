## Para visualizar o Dashboard:  
https://dash-sales-retail-duckdb.onrender.com/  
  

## Passos para instalação o projeto:  
instalar poetry caso não tenha instalado:  
`pip install poetry` 

Clonar o repositório:  
`git clone https://github.com/thomasfsr/transform_duck.git`  

entrar no repositório:  
`cd transform_duck`  
  
instalar pyenv e utilizar python 3.11.5 no diretório root do projeto:  
`pyenv local 3.11.5`  
  
instalar as dependências com poetry:  
`poetry install`  
  
Baixar os arquivos csv e colocar em um diretorio data dentro do projeto.  
https://drive.google.com/drive/folders/1D8fsqnVaB97OYHr03buo8SqelD4zlkUX  
  
rodar o start.py que criará o database a partir dos csv's do diretório data:  
`task run`  
Para rodar o dashboard local:  
`task dash`  
Para formatar segundo o padrão blue e isort:  
`task format`  
Para rodar os testes:  
`task test`  
