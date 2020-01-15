def run(data, parameters):
    print ('parameters for this script are: ', parameters)
    try:
        if parameters["animal"] == 'true':
            data = "<img crossorigin='anonymous' src='https://upload.wikimedia.org/wikipedia/commons/thumb/6/66/An_up-close_picture_of_a_curious_male_domestic_shorthair_tabby_cat.jpg/1280px-An_up-close_picture_of_a_curious_male_domestic_shorthair_tabby_cat.jpg' class='jpg mw-mmv-dialog-is-open' alt='' width='639' height='639' style=''>"
        else:
            data = "<img src='https://cdn.pixabay.com/photo/2018/05/07/10/48/husky-3380548__340.jpg'>"
        return data
    except:
        return "<img crossorigin='anonymous' src='https://upload.wikimedia.org/wikipedia/commons/thumb/6/66/An_up-close_picture_of_a_curious_male_domestic_shorthair_tabby_cat.jpg/1280px-An_up-close_picture_of_a_curious_male_domestic_shorthair_tabby_cat.jpg' class='jpg mw-mmv-dialog-is-open' alt='' width='639' height='639' style=''>"
