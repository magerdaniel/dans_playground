# Getting Started

Welcome to Dan's AI on BI Playground! This guide will help you set up your environment and start learning.

## Prerequisites

- Python 3.8 or higher
- Git
- Basic understanding of Python programming
- Familiarity with SQL and databases (helpful)
- Interest in AI/ML and Business Intelligence

## Initial Setup

### 1. Clone the Repository

```bash
git clone https://github.com/magerdaniel/dans_playground.git
cd dans_playground
```

### 2. Set Up Python Environment

Create a virtual environment to keep dependencies isolated:

```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### 3. Install Common Dependencies

As you work on projects, you'll install specific dependencies. Here are some common ones:

```bash
# Basic data science stack
pip install pandas numpy matplotlib seaborn

# Jupyter for notebooks
pip install jupyter jupyterlab

# AI/ML libraries
pip install openai langchain

# Database connectivity
pip install sqlalchemy psycopg2-binary pymysql

# API utilities
pip install requests python-dotenv
```

### 4. Set Up Environment Variables

Create a `.env` file in the project root for API keys and secrets (this file is gitignored):

```bash
# .env file example
OPENAI_API_KEY=your_api_key_here
TABLEAU_TOKEN=your_token_here
DATABASE_URL=your_database_url_here
```

## Learning Path

### Phase 1: Foundations
1. Understand BI system architectures
2. Learn API authentication methods
3. Practice basic database queries
4. Get familiar with data formats (JSON, CSV, etc.)

### Phase 2: AI Basics
1. Learn about LLMs and their capabilities
2. Experiment with prompt engineering
3. Understand embeddings and vector search
4. Try simple text-to-SQL conversions

### Phase 3: Integration
1. Connect to a BI platform API
2. Retrieve and parse data
3. Build a simple natural language query interface
4. Create an automated insight generator

### Phase 4: Advanced Projects
1. Build a chatbot for dashboard interactions
2. Implement real-time anomaly detection
3. Create automated reporting systems
4. Develop predictive analytics features

## Directory Usage

- **docs/**: Document what you learn. Write tutorials, take notes, create guides.
- **examples/**: Create self-contained example projects demonstrating specific concepts.
- **notebooks/**: Use for experimentation and exploration. Great for learning interactively.
- **scripts/**: Build reusable utilities and tools you'll use across projects.
- **resources/**: Store reference materials, sample data, and documentation.

## Best Practices

### Code Organization
- Keep projects self-contained
- Write clear documentation
- Include requirements files
- Add comments explaining complex logic

### Data Security
- Never commit API keys or credentials
- Use environment variables for secrets
- Don't commit sensitive data
- Review `.gitignore` regularly

### Learning Approach
- Start small and build incrementally
- Document your experiments
- Don't be afraid to break things
- Review and refactor code regularly

## Quick Start Projects

### Project 1: Hello BI API
Create a simple script that connects to a BI platform API and retrieves basic information.

### Project 2: Natural Language to SQL
Build a simple converter that takes natural language questions and generates SQL queries.

### Project 3: Data Insights Generator
Create a tool that analyzes data and generates written insights using an LLM.

## Resources

### Official Documentation
- Python: https://docs.python.org/3/
- OpenAI API: https://platform.openai.com/docs
- Pandas: https://pandas.pydata.org/docs/
- SQLAlchemy: https://docs.sqlalchemy.org/

### Tutorials and Guides
- Real Python: https://realpython.com/
- Towards Data Science: https://towardsdatascience.com/
- AI/ML tutorials: https://www.tensorflow.org/tutorials

## Troubleshooting

### Common Issues

**Import errors**: Make sure your virtual environment is activated and dependencies are installed.

**API authentication failures**: Check your `.env` file and ensure API keys are correct.

**Database connection issues**: Verify connection strings and network access.

## Next Steps

1. Set up your development environment following this guide
2. Explore the `docs/` directory for learning materials
3. Try running example notebooks in `notebooks/`
4. Start with a simple example project in `examples/`
5. Build your own experiments!

---

Happy learning! 🚀
