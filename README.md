## Passos para instalar o projeto:  
`git clone https://github.com/thomasfsr/transform_duck.git`  

instalar pyenv e utilizar python 3.11.5 no diretório root do projeto:  
`pyenv local 3.11.5`  
  
instalar poetry:  
`pip install poetry`  
  
instalar as dependencias com poetry:  
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
