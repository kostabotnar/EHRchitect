class TnxMapColumns:
    file = "File"
    table = "Table"
    column = "Column"
    tnx_file = "TNX File"
    tnx_column = "TNX Column"


class DataDictionaryColumns:
    file_name = "File Name"
    table_name = "Table Name"
    column_name = "Column Name"
    data_type = "Data Type"
    length = "Length"
    nullable = "Nullable"
    primary_key = "Primary Key"
    foreign_key = "Foreign Key"
    index = "Index"

    fk_separator = "|"

    val_yes = 'Yes'
    val_no = 'No'


class CommonColumns:
    patient_id = "patient_id"
    encounter_id = "encounter_id"
    code_system = "code_system"
    code = "code"
    date = "date"
    start_date = "start_date"
    end_date = "end_date"
    type = "type"
    num_value = "num_value"
    text_value = "text_value"
    units_of_measure = "units_of_measure"
    route = "route"
    brand = "brand"
    strength = "strength"
    sex = "sex"
    race = "race"
    ethnicity = "ethnicity"
    marital_status = "marital_status"
    date_of_birth = "date_of_birth"
    date_of_death = "date_of_death"
    code_description = "code_description"
    # ICD map
    icd9_code = 'icd9_code'
    icd10_code = 'icd10_code'
    description = 'description'
    # chain result
    event_id = 'event_id'
    event_name = 'event_name'
    category = 'category'
    level = 'level'
    time_interval = "t"

    date_columns = ["date", "start_date", "end_date", "date_of_birth", "date_of_death"]

    @staticmethod
    def get_column_at_level(column_name: str, level: int) -> str:
        return f'{column_name}_{level}'


class CommonTables:
    patient = "patient"
    encounter = "encounter"
    diagnosis = "diagnosis"
    medication = "medication"
    lab_result = "lab_result"
    procedures = "procedures"
    vital_sign = "vital_sign"
    code_description = "code_description"
    icd9_map_icd10 = "icd9_map_icd10"
