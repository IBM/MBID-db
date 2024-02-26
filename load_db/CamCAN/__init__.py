def years_education_convertion(subject_data):
    """
    Map the education specified in the approved_data.tsv file to a number of years
    based on the information provided by
    https://www.brightworldguardianships.com/en/guardianship/british-education-system/
    """
    if subject_data["phd_doc_years"] or subject_data["masters_years"]:
        return 16 + (subject_data["masters_years"] or 0) + (subject_data["phd_doc_years"] or 0)
    if subject_data["undergrad_years"] or subject_data["btec_ad_years"]:
        return 13 + (subject_data["undergrad_years"] or 0) + (subject_data["btec_ad_years"] or 0)
    if subject_data["col_dip_years"] or subject_data["hnc_hnd_nvq4_btecp_years"] or subject_data["a_level_ib_years"]:
        return 11 + (subject_data["col_dip_years"] or 0) + (subject_data["hnc_hnd_nvq4_btecp_years"] or 0) +\
            (subject_data["a_level_ib_years"] or 0)
    if subject_data["o_level_gcse_leaving_years"] or subject_data["cse_years"]:
        return 11
    if subject_data["nvq3_btecd_years"]:
        return 10
    if subject_data["nvq2_btec1_years"]:
        return 9
    if subject_data["nvq1_bteci_years"]:
        return 8
    return None