def validate_columns(df, config, file_name, error_logs):

    # check if schema is defined for this file
    if file_name not in config:
        error_logs.append({
            "error_type": "MissingSchemaDefinition",
            "file": file_name,
            "message": f"No schema definition found in config for '{file_name}'"
        })
        return None  # or raise Exception if we want to stop here
        # raise ValueError(f"Schema not found for file: {file_name}")
    
    file_config = config[file_name]
    expected_cols = list(file_config.get('column_types', {}).keys())
    required_cols = file_config.get('required_columns', []) 

    df_cols = df.columns.tolist()
    extra = list(set(df_cols) - set(expected_cols))
    missing = list(set(required_cols) - set(df_cols))

    if extra:
        error_logs.append({
            "error_type": "ExtraColumns",
            "file": file_name,
            "extra_columns": extra
        })

    if missing:
        error_logs.append({
            "error_type": "MissingColumns",
            "file": file_name,
            "missing_columns": missing
        })

    return file_config