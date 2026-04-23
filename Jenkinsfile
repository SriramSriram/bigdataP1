pipeline {
    agent any

    environment {
        PYTHON = 'python'
    }

    stages {

        // ─────────────────────────────────────────
        // STAGE 1 — Checkout code from GitHub
        // ─────────────────────────────────────────
        stage('Checkout') {
            steps {
                echo 'Checking out code from GitHub...'
                checkout scm
            }
        }

        // ─────────────────────────────────────────
        // STAGE 2 — Install Dependencies
        // ─────────────────────────────────────────
        stage('Install Dependencies') {
            steps {
                echo 'Installing Python dependencies...'
                sh '''
                    python --version
                    pip install requests kafka-python pyspark pandas --quiet --break-system-packages
            
                    # Create required folders that are in .gitignore
                    mkdir -p logs
                    mkdir -p data/raw
                    mkdir -p data/processed
                '''
            }
        }

        // ─────────────────────────────────────────
        // STAGE 3 — Data Ingestion
        // ─────────────────────────────────────────
        stage('Stage 1 - Ingest Data') {
            steps {
                echo 'Running ingestion script...'
                sh '''
                    cd ingestion
                    python ingest.py
                '''
            }
        }

        // ─────────────────────────────────────────
        // STAGE 4 — Data Quality Cleaning
        // ─────────────────────────────────────────
        stage('Stage 2 - Clean Data') {
            steps {
                echo 'Running PySpark cleaning job...'
                sh '''
                    cd cleaning
                    python clean.py
                '''
            }
        }

        // ─────────────────────────────────────────
        // STAGE 5 — Build Dashboard
        // ─────────────────────────────────────────
        stage('Stage 5 - Build Dashboard') {
            steps {
                echo 'Building HTML dashboard...'
                sh '''
                    cd dashboard
                    python dashboard.py
                '''
            }
        }

        // ─────────────────────────────────────────
        // STAGE 6 — Archive Output
        // ─────────────────────────────────────────
        stage('Archive Results') {
            steps {
                echo 'Archiving dashboard output...'
                archiveArtifacts artifacts: 'dashboard/energy_dashboard.html',
                                 fingerprint: true
            }
        }
    }

    // ─────────────────────────────────────────
    // POST BUILD NOTIFICATIONS
    // ─────────────────────────────────────────
    post {
        success {
            echo 'Pipeline completed successfully!'
        }
        failure {
            echo 'Pipeline failed. Check logs above.'
        }
    }
}