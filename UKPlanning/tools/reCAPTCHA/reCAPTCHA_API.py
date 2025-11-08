import time
from selenium.webdriver.common.action_chains import ActionChains  # for click reCAPTCHA buttons.
from selenium.webdriver.common.by import By
from tools.reCAPTCHA.reCAPTCHA_model import predict_base64_image

def solve_puzzle(driver):
    time.sleep(1) # wait for puzzle keyword
    n_clocks = 0
    puzzle_keyword = driver.find_element(By.XPATH, '//*[@id="root"]/div/form/div[3]/div/div[1]/em').text.strip()
    while n_clocks != 5:
        # Refresh for a clock puzzle.
        while puzzle_keyword not in ['clock', '时钟']: # add the default languages of your browser here.
            driver.find_element(By.XPATH, '//*[@id="amzn-btn-refresh-internal"]').click()  # click refresh button.
            time.sleep(1) # wait for refreshing a new puzzle
            puzzle_keyword = driver.find_element(By.XPATH, '//*[@id="root"]/div/form/div[3]/div/div[1]/em').text.strip()
        print(f'puzzle_keyword: {puzzle_keyword}')

        # Get puzzle image
        canvas = driver.find_element(By.TAG_NAME, "canvas")
        base64_str = driver.execute_script("return arguments[0].toDataURL('image/png');", canvas)
        #print(base64_str)

        # Solve this puzzle with pre-trained neural network model, get the indexes of clock images.
        solution = predict_base64_image(base64_str, model_path="tools/reCAPTCHA/models/image_classifier.h5", class_file="tools/reCAPTCHA/class_names.txt")
        n_clocks = len(solution)  # Need to confirm that n_clock is 5.
        puzzle_keyword = 0 # if n_clocks !=5 (failed to solve this puzzle), will refresh puzzle, as puzzle_keyword != clock.
        # print(solution)
    return solution


def click_puzzle_buttons(driver, result):
    # Click puzzle results.
    for button_id in result:
        # option1: Use Javascript to click buttons.
        button = driver.find_element(By.XPATH, f'//*[@id="root"]/div/form/div[3]/div/div[2]/canvas/button[{button_id}]')
        driver.execute_script("arguments[0].click();", button)
        # option2: Use selenium to click buttons.
        # [DISCARD: selenium.common.exceptions.MoveTargetOutOfBoundsException: Message: move target out of bounds]
        # click_puzzle_button(driver, button_id)
        time.sleep(0.5)
    # Submit puzzle results.
    driver.find_element(By.XPATH, '//*[@id="amzn-btn-verify-internal"]').click()

    # DISCARD:
    def click_puzzle_button(driver, button_id):
        canvas = driver.find_element(By.XPATH, '//*[@id="root"]/div/form/div[3]/div/div[2]/canvas')
        loc = canvas.location
        size = canvas.size
        print(f'canvas info, loc: {loc}, size: {size}')
        w, h = size['width'] / 3, size['height'] / 3
        i, j = divmod(button_id-1, 3)
        x = loc['x'] + (j + 0.5) * w
        y = loc['y'] + (i + 0.5) * h
        print(f'click button_id: {button_id}-[{i}, {j}], x: {x}, y: {y}.')
        actions = ActionChains(driver)
        actions.move_by_offset(x, y).click().perform()
