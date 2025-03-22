CREATE TABLE IF NOT EXISTS diabetes_dataset (
    id SERIAL PRIMARY KEY,
    -- The age of the individual (18-90 years).
    Age INT, 
    -- Number of times the patient has been pregnant.
    Pregnancies INT,
    -- A measure of body fat based on height and weight (kg/m²).
    BMI FLOAT,
    -- Blood glucose concentration (mg/dL), a key diabetes indicator.
    Glucose FLOAT,
    -- Systolic blood pressure (mmHg), higher levels may indicate hypertension.
    BloodPressure FLOAT,
    -- Hemoglobin A1c level (%), representing average blood sugar over months.
    HbA1c FLOAT,
    -- LDL (Low-Density Lipoprotein): "Bad" cholesterol level (mg/dL).
    LDL FLOAT,
    -- HDL (High-Density Lipoprotein): "Good" cholesterol level (mg/dL).
    HDL FLOAT,
    -- Fat levels in the blood (mg/dL), high values increase diabetes risk.
    Triglycerides FLOAT,
    -- Waist measurement (cm), an indicator of central obesity.
    WaistCircumference FLOAT,
    -- Hip measurement (cm), used to calculate WHR.
    HipCircumference FLOAT,
    -- WHR (Waist-to-Hip Ratio): Waist circumference divided by hip circumference.
    WHR FLOAT,
    -- Indicates if the individual has a family history of diabetes
    FamilyHistory BOOLEAN,
    -- Dietary habits.
    DietType VARCHAR(20) CHECK (DietType IN ('Unbalanced', 'Balanced', 'Vegan/Vegetarian')),
    -- Presence of high blood pressure.
    Hypertension BOOLEAN,
    -- Indicates if the individual is taking medication.
    MedicationUse BOOLEAN,
    -- Diabetes diagnosis result.
    Outcome BOOLEAN
);
