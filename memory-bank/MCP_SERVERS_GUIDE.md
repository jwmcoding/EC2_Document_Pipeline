# MCP Servers Guide

> **Quick Reference**: Available Model Context Protocol (MCP) servers for this project and how to use them with Cursor IDE.

## Overview

MCP servers extend Cursor's capabilities by providing specialized tools for different domains. This project currently uses:

| Server | Purpose | Status |
|--------|---------|--------|
| **Pinecone MCP** | Vector database operations, search, indexing | ✅ In use (documented in AGENTS.md) |
| **AWS MCP (Core)** | AWS service guidance, architecture patterns | ✅ Working (tested Dec 7, 2025) |
| **Context7** | Library documentation lookup | ✅ Available |
| **LangChain Docs** | LangChain framework reference | ✅ Available |
| **shadcn/ui** | React component library reference | ✅ Available |

---

## 🔧 AWS MCP Server

**Status**: ✅ Tested and working

### What It Does

The AWS MCP server provides comprehensive AWS guidance including:

- **Service Selection**: Map requirements to appropriate AWS services
- **CLI Commands**: Suggest and execute AWS CLI commands
- **Architecture Patterns**: Get best practices for specific use cases
- **Cost Analysis**: Pricing and cost optimization guidance
- **Infrastructure Code**: CDK, Terraform, CloudFormation patterns

### Available Tools

| Tool | Purpose |
|------|---------|
| `prompt_understanding` | Analyze requirements, suggest AWS services |
| `suggest_aws_commands` | Find relevant AWS CLI commands for a task |
| `call_aws` | Execute AWS CLI commands directly |
| Service-specific tools | For EC2, RDS, Lambda, S3, DynamoDB, etc. |

### When to Use

✅ **Use for:**
- Finding the right AWS service for a problem
- Getting AWS CLI commands for tasks
- Understanding architectural patterns
- Cost and performance optimization
- Infrastructure setup questions

❌ **Don't use for:**
- General coding questions (use regular coding tools)
- Non-AWS cloud platforms
- Real-time monitoring (use CloudWatch directly)

### Example Queries

```
"Find all running EC2 instances in us-west-2"
"How do I set up a serverless RAG system with Bedrock and DynamoDB?"
"What's the best way to deploy a Docker container to AWS?"
"Get pricing for S3, EC2, and Lambda for January 2025"
```

### Integration with This Project

**Current Use Case**: AWS EC2 deployment for document processing pipeline

```bash
# Example from our workflow
aws ec2 start-instances --instance-ids i-0c55fdf7fb3f660d8 --region us-east-2
ssh -i ~/Downloads/docking.pem ec2-user@18.221.163.252
aws s3 sync s3://bucket/ /local/path/
```

---

## 📚 Pinecone MCP Server

**Status**: ✅ In use (documented in AGENTS.md)

### Key Points

- **Always use SDK** for data operations (upsert, query, delete, search)
- **Use CLI** for index creation/deletion/configuration
- **Namespaces required** for all operations (data isolation)
- **Reranking recommended** for production search quality
- **Batch limits**: 96 records max per batch for text

See `AGENTS.md` for complete Pinecone quick reference.

---

## 🔍 Context7 MCP Server

**Status**: ✅ Available

### Purpose

Look up current documentation and code examples for any library/framework.

### When to Use

```
"Get documentation for Next.js v14 hooks"
"Show me FastAPI dependency injection examples"
"What's the current Anthropic Claude API for function calling?"
```

---

## 📖 LangChain Docs MCP Server

**Status**: ✅ Available

### Purpose

Search LangChain documentation for agents, chains, tools, and patterns.

### When to Use

```
"How do I create a LangGraph agent with tool calling?"
"Show me RAG patterns in LangChain"
```

---

## 🎨 shadcn/ui MCP Server

**Status**: ✅ Available

### Purpose

Get component source code, demos, and metadata for shadcn/ui v4.

### When to Use

```
"Get the Button component source code"
"Show me how to use the Dialog component"
"What components are available in shadcn/ui?"
```

---

## Best Practices

### 1. **Use Specialized Tools First**

Don't ask general questions; use service-specific MCP servers:

```
❌ Bad: "Can you help me with AWS?"
✅ Good: "How do I create an EC2 instance with a GPU?"
```

### 2. **Document MCP Usage**

When an MCP server provides critical information for a task, note it:

```markdown
Deployed to AWS using documentation from AWS MCP server.
- Instance type: g5.4xlarge (GPU support)
- Region: us-east-2
- Elastic IP: Configured for persistence
```

### 3. **Combine Tools**

Use multiple MCP servers for complex tasks:

1. **AWS MCP** → Get service architecture
2. **Context7** → Get library documentation  
3. **Regular tools** → Write code implementation

### 4. **Keep Documentation Current**

Update this guide when:
- New MCP servers are added
- Server status changes
- New patterns are discovered

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| MCP server not responding | Check if server is installed and authenticated |
| AWS commands failing | Verify AWS credentials are set (`aws sts get-caller-identity`) |
| Rate limiting | Use exponential backoff for CLI commands |
| Documentation outdated | Use Context7 to fetch latest docs |

---

## Next Steps

- Monitor AWS MCP performance for document processing pipeline
- Evaluate adding more specialized MCP servers as needed
- Document patterns discovered through MCP guidance

---

**Last Updated**: December 7, 2025
**Created**: December 7, 2025 (Testing AWS MCP Server)














