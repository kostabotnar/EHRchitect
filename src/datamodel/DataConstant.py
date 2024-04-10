from enum import Enum


class CodeSystem:
    # code system
    icd9cm = 'ICD-9-CM'
    icd10cm = 'ICD-10-CM'
    loinc = 'LOINC'
    tnx = 'TNX'
    ndc = 'NDC'
    RxNorm = 'RxNorm'
    cpt = 'CPT'
    hcpcs = 'HCPCS'
    icd10pcs = 'ICD-10-PCS'
    snomed = 'SNOMED'


class EncounterType(Enum):
    unknown = 'UNKNOWN'
    amb = 'AMB'
    emer = 'EMER'
    imp = 'IMP'
    ss = 'SS'
    vr = 'VR'
    prenc = 'PRENC'
    obsenc = 'OBSENC'
    nonac = 'NONAC'
