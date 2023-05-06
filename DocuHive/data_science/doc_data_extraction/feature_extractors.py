import numpy as np
import cv2


def get_vert_split_gaps(grey_image, img_height, min_height_percent=0.3):
    thresh_val = np.mean(grey_image) * 0.7
    _, thresh = cv2.threshold(grey_image, thresh_val, 255, cv2.THRESH_BINARY)

    min_height = img_height * min_height_percent  # Minimum height of line to consider
    vertical_lines = [None]
    gaps = [[0, 0, 0, img_height]]
    for i, col_idx in enumerate(range(thresh.shape[1])):
        col = thresh[:, col_idx]
        init_color = col[0]
        count = 0
        for color in col:
            if count > min_height:
                if vertical_lines[-1] is None:
                    vertical_lines[-1] = col
                else:
                    vertical_lines[-1] = np.column_stack((vertical_lines[-1], col))
                gaps[-1][2] = i
                break
            if color != init_color:
                init_color = color
                count = 0
            count += 1
        else:
            if vertical_lines[-1] is not None:
                vertical_lines.append(None)
                gaps.append([i, 0, i, img_height])
            gaps[-1][0] = i
            gaps[-1][2] = i

    return gaps


def get_max_mid_page_gap(gaps, img_width, width=10):
    gaps = [b for b in gaps if (b[2] - b[0]) > width and (img_width * 0.12 < (b[2] + b[0]) / 2 < img_width * 0.88)]
    max_gap = None
    if len(gaps) >= 1:
        max_gap = gaps[0]
        for b in gaps:
            if b[2] - b[0] > max_gap[2] - max_gap[0]:
                max_gap = b
    return max_gap


def debug_vertical_gap(img, gap):
    if gap is not None:
        cv2.rectangle(
            img,
            (gap[0], gap[1]),
            (gap[2], gap[3]),
            (255, 255, 0),
            2,
        )
    return img


def get_red_rectangles(cv2_image):
    image = cv2.cvtColor(cv2_image, cv2.COLOR_BGR2HSV)
    lower = np.array([110, 25, 0])
    upper = np.array([115, 255, 255])
    mask = cv2.inRange(image, lower, upper)
    contours, hierarchy = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    filtered_contours = []
    for contour in contours:
        polygon = cv2.approxPolyDP(contour, 0.05 * cv2.arcLength(contour, True), True)
        if len(polygon) == 4 and cv2.isContourConvex(polygon):
            x, y, w, h = cv2.boundingRect(contour)
            if 4 < w < 8 and 40 < h < 50 and 750 < x < 780:
                filtered_contours.append([x, y, x + w, y + h])
    return filtered_contours
