# Workflow Template com Dataproc no Google Cloud

Este repositório contém um projeto para provisionamento de infraestrutura utilizando Terraform. O objetivo é criar um Workflow Template no Dataproc com agendamento automático configurado pelo Google Cloud Scheduler.

## Funcionalidades

- **Provisionamento do Dataproc Workflow Template**: Cria um template que define a execução de jobs no Dataproc.
- **Configuração do Cloud Scheduler**: Agenda execuções periódicas para o Workflow Template.
- **Criação de Buckets no Cloud Storage**: Configura buckets para entrada e saída de dados no Dataproc.
- **Upload de Arquivos para o Bucket**: Realiza o upload automático de arquivos especificados para o bucket criado.
- **Gerenciamento de Contas de Serviço**: Cria uma conta de serviço personalizada e papéis IAM para executar o Workflow Template.

## Pré-requisitos

1. Conta no Google Cloud com permissões suficientes para criar recursos (Dataproc, Cloud Scheduler, etc.).
2. CLI do Google Cloud instalada e configurada.
3. Terraform instalado na máquina local.
4. Projeto no Google Cloud criado e com faturamento habilitado.

### Contas de Serviço e Papéis IAM

- **Conta de Serviço**: Criada com o recurso `google_service_account`.
  - Nome: Configurado com base nas variáveis `template_name` e `project_id`.
  - Descrição: "conta de serviço para o projeto".
- **Papel Personalizado (Custom Role)**: Criado para gerenciar permissões de execução do Workflow Template.
  - Permissões: `dataproc.workflowTemplates.instantiate` e `iam.serviceAccounts.actAs`.
- **Vínculo IAM**: Associa a conta de serviço criada ao papel personalizado.