# Pipeline Transformers

Компоненты для трансформации данных в pipeline фреймворке.

## Установка

```bash
# Базовая установка
pip install pipeline-transformers

# С дополнительными зависимостями
pip install pipeline-transformers[processing,text,image,stats,timeseries,geo,all]
```

## Доступные трансформеры

### Data Transformers

#### Data Transformer
Универсальный трансформер для базовых операций с табличными данными.

```yaml
transform_data:
  type: "data-transformer"
  config:
    source_stage: "extract"
    
    # Операции с колонками
    add_columns:
      full_name: "first_name + ' ' + last_name"
      age_group: "case when age < 30 then 'Young' when age < 50 then 'Middle' else 'Senior' end"
      annual_salary: "salary * 12"
    
    drop_columns: ["temp_field", "internal_id"]
    
    rename_columns:
      emp_name: "employee_name"
      dept: "department"
    
    # Фильтрация данных
    filter_expression: "salary > 30000 and active == True"
    
    # Обработка пропущенных значений
    fill_na_strategy: "mean"  # keep, drop, mean, median, mode, forward, backward, value
    fill_na_value: 0  # для стратегии "value"
    
    # Типы данных
    convert_dtypes:
      created_at: "datetime"
      category: "category"
      score: "float64"
    
    # Сортировка
    sort_by: ["salary", "age"]
    sort_ascending: [false, true]
    
    # Дедупликация
    drop_duplicates: true
    duplicate_subset: ["email"]  # опционально
    
    # Сэмплирование
    sample_frac: 0.1  # взять 10% записей
    random_state: 42
    
    output_format: "pandas"
```

**Основные возможности:**
- Добавление вычисляемых колонок через pandas expressions
- Удаление и переименование колонок
- Фильтрация данных через pandas query
- Обработка пропущенных значений (различные стратегии)
- Преобразование типов данных
- Сортировка и дедупликация
- Сэмплирование данных

#### Aggregation Transformer (планируется)
Агрегация и группировка данных.

```yaml
aggregate:
  type: "aggregation-transformer"
  config:
    source_stage: "transform"
    group_by: ["department", "level"]
    aggregations:
      salary: ["mean", "median", "std"]
      count: "size"
      max_age: "age.max()"
```

#### Join Transformer (планируется)
Объединение данных из нескольких источников.

```yaml
join_data:
  type: "join-transformer"
  config:
    left_stage: "users"
    right_stage: "departments"
    join_type: "left"  # left, right, inner, outer
    left_on: "dept_id"
    right_on: "id"
```

### ML Transformers

#### ML Transformer (планируется)
Применение ML моделей для трансформации данных.

```yaml
ml_transform:
  type: "ml-transformer"
  config:
    source_stage: "preprocess"
    operations:
      - type: "scaler"
        method: "standard"  # standard, minmax, robust
        columns: ["age", "salary"]
      
      - type: "encoder"
        method: "onehot"  # onehot, label, target
        columns: ["department"]
      
      - type: "feature_selection"
        method: "variance"
        threshold: 0.1
      
      - type: "predict"
        model_path: "models/classifier.pkl"
        output_column: "prediction"
```

### Text Transformers

#### Text Transformer (планируется)
Обработка текстовых данных.

```yaml
text_transform:
  type: "text-transformer"  
  config:
    source_stage: "extract"
    text_column: "description"
    operations:
      - type: "clean"
        lowercase: true
        remove_punctuation: true
        remove_stopwords: true
      
      - type: "tokenize"
        method: "word"  # word, sentence, custom
      
      - type: "vectorize"
        method: "tfidf"  # tfidf, count, word2vec
        max_features: 1000
      
      - type: "sentiment"
        output_column: "sentiment"
```

## Базовые возможности

Все трансформеры наследуют от `BaseTransformer` и поддерживают:

### Batch обработка
```yaml
config:
  batch_size: 10000
  parallel: true
  n_jobs: 4
```

### Обработка ошибок
```yaml
config:
  error_handling: "skip"  # raise, skip, default, log
  skip_errors: true
  validate_input: true
  validate_output: true
```

### Форматы вывода
```yaml
config:
  output_format: "pandas"  # pandas, polars, dict, list
```

## Создание кастомного трансформера

```python
from pipeline_transformers import BaseTransformer, TransformerConfig
from pipeline_core import register_component, ExecutionContext
from pydantic import Field
import pandas as pd

class MyTransformerConfig(TransformerConfig):
    type: str = Field(default="my-transformer", const=True)
    multiplier: float = Field(default=1.0, description="Множитель для числовых колонок")

@register_component("my-transformer") 
class MyTransformer(BaseTransformer):
    def get_config_model(self):
        return MyTransformerConfig
    
    def transform_data(self, data, context: ExecutionContext):
        config = self.config
        
        # Преобразуем в DataFrame если нужно
        if not isinstance(data, pd.DataFrame):
            df = pd.DataFrame(data)
        else:
            df = data.copy()
        
        # Применяем трансформацию
        numeric_columns = df.select_dtypes(include=['number']).columns
        df[numeric_columns] = df[numeric_columns] * config.multiplier
        
        return df
```

## Примеры использования

### Простая трансформация
```python
from pipeline_transformers import DataTransformerComponent
from pipeline_core import ExecutionContext, ExecutionResult, ExecutionStatus

# Подготавливаем данные
import pandas as pd
data = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'salary': [50000, 60000, 70000]
})

# Конфигурация трансформера
config = {
    "type": "data-transformer",
    "source_stage": "extract",
    "add_columns": {
        "age_group": "case when age < 30 then 'Young' else 'Senior' end",
        "monthly_salary": "salary / 12"
    },
    "filter_expression": "age >= 30",
    "output_format": "pandas"
}

# Выполняем трансформацию
transformer = DataTransformerComponent(config)

context = ExecutionContext()
context.set_stage_result("extract", ExecutionResult(
    status=ExecutionStatus.SUCCESS,
    data=data,
    processed_records=len(data)
))

result = transformer.execute(context)
print(result.data)
```

### Цепочка трансформаций
```yaml
# pipeline.yml
stages:
  - extract
  - clean
  - enrich
  - aggregate

extract:
  type: "csv-extractor"
  config:
    file_path: "sales_data.csv"

clean:
  type: "data-transformer"
  depends_on: ["extract"]
  config:
    source_stage: "extract"
    drop_columns: ["temp_field"]
    fill_na_strategy: "mean"
    filter_expression: "amount > 0"

enrich:
  type: "data-transformer"
  depends_on: ["clean"]
  config:
    source_stage: "clean"
    add_columns:
      profit_margin: "(revenue - cost) / revenue"
      quarter: "pd.to_datetime(date).dt.quarter"

aggregate:
  type: "aggregation-transformer"
  depends_on: ["enrich"]
  config:
    source_stage: "enrich"
    group_by: ["quarter", "region"]
    aggregations:
      total_revenue: "revenue.sum()"
      avg_margin: "profit_margin.mean()"
```

## Развитие

Планируемые трансформеры:
- **Advanced Data**: Pivot, Unpivot, Window functions
- **ML**: Feature engineering, Model inference, Ensemble
- **Text**: NLP pipeline, Entity extraction, Classification
- **Image**: Resize, Filters, Feature extraction
- **Time Series**: Resampling, Smoothing, Forecasting
- **Geospatial**: Projections, Distance calculations, Clustering

## Лицензия

Apache License 2.0