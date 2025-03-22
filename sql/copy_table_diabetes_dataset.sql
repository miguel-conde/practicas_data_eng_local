COPY diabetes_dataset (Age, Pregnancies, BMI, Glucose, BloodPressure, HbA1c, LDL, HDL, 
    Triglycerides, WaistCircumference, HipCircumference, WHR, FamilyHistory, DietType, 
    Hypertension, MedicationUse, Outcome)
FROM STDIN WITH (FORMAT csv);