# Boticario

[![CI](https://github.com/paulosdchaves/boticario-case/workflows/CI/badge.svg?branch=master)](https://github.com/paulosdchaves/boticario-case/actions?query=workflow:CI)
[![codecov](https://codecov.io/gh/paulosdchaves/boticario-case/branch/master/graph/badge.svg?token=byCb4nFNWv)](https://codecov.io/gh/paulosdchaves/boticario-case)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
<a href="https://github.com/paulosdchaves/boticario-case/graphs/traffic">
<img src="https://visitor-badge.glitch.me/badge?page_id=paulosdchaves.boticario-case">
</a>

## The project

Projeto voltado para a resolução do case da Boticario, usando as melhores práticas e técnicas de desenvolvimento para a criação do ambiente local integrado com a Google Cloud Platform (GCP), criação de features usando técnicas de TDD, Dynamically Generating DAGs e CI/CD para deploy automatizado usando o GitHub Actions.

## Quickstart

Clonar o projeto em seu ambiente local:

```shell script
git clone https://github.com/paulosdchaves/boticario-case.git
```

# Executando Airflow localmente

![local_desktop_airflow.png](/docs/local_desktop_airflow.png)

Para rodar o Airflow localmente você precisará de:

- Pelo menos 4G de RAM disponíveis
- Banda larga para baixar imagens Docker

### Dependências?
Docker, docker-compose and makefile.

### Como rodar?

O comando abaixo configurará o ambiente usando o docker-compose para a instancia do PostgresSQL e o Airflow inicializara suas configurações internas, criação das credenciais e conexões.
```bash
make setup
```
Ao executar o comando acima, é possível acessar o Airflow em `localhost: 8080`. 
Um usuário de testes é criado user: admin / password: admin.
O comando abaixo irá popular as tabelas no banco de dados PostgreSQL local e irá rodar os testes:
```bash
make test
```

Para checar o código, execute: `make lint`

Para formata o código, execute: `make format`

## Arquitetura final

![case.png](/docs/case.png)

