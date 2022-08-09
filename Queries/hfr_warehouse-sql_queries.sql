SELECT DISTINCT FP."Facility_Key",
	INSTI."Facility_Name"
FROM FACT_PERSONNEL AS FP
JOIN DIM_INSTITUTIONS AS INSTI ON FP."Facility_Key" = INSTI."Facility_Key"
WHERE "Number_of_Doctors" > 0
ORDER BY "Facility_Key";