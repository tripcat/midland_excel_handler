from buyerseller_test import namestring_analyzer
bs_type = 's'
full_bs_info = "HO YING HO 66 /100, HO KING CHEUNG 34 7100"
#full_bs_info = "TAM TZE CHIU LAW FUN WAH TAM KWOK KING"
full_bs_info = "WONG CHI YING 3 /5, YUE SZE MAN5, LEUNG KWOK YEE I"
list_name = namestring_analyzer(full_bs_info, bs_type)
print(list_name)