import cv2


def get_cv2_coords(b):
    point1 = (int(b[0]), int(b[1]))
    point2 = (int(b[2]), int(b[3]))
    return point1, point2, [int(b[0]), int(b[1]), int(b[2]), int(b[3])]


def write_text_on_coord(img, polygon, text):
    text_font_size = img.shape[1] / 3000
    x1, y1, x2, y2 = polygon
    c1 = (x1, y1)
    c2 = (x2, y2)
    cv2.rectangle(img, c1, c2, (255, 255, 255), -1)
    font = cv2.FONT_HERSHEY_SIMPLEX
    text_size, _ = cv2.getTextSize(text, font, 1, 2)
    line_height = text_size[1] + 5
    for i, text in enumerate(text.split("\n")):
        y = y1 + 15 + i * line_height
        cv2.putText(img, text, (x1 + 3, y), font, text_font_size, (0, 0, 0), 2)
    return img


def draw_polygons(img, polygons, color=(0, 255, 0), thickness=1):
    for p in polygons:
        box = p[:4]
        c1, c2, _ = get_cv2_coords(box)
        # ""
        cv2.rectangle(img, c1, c2, color, thickness)
    return img


def show_image(img):
    cv2.imshow("image", img)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
