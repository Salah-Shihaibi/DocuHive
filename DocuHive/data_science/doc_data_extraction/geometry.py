import math


def rectangles_do_not_collide(rect1, rect2):
    r1x1, r1y1, r1x2, r1y2 = rect1
    r2x1, r2y1, r2x2, r2y2 = rect2
    if r1x2 <= r2x1 or r1x1 >= r2x2 or r1y2 <= r2y1 or r1y1 >= r2y2:
        return True
    else:
        return False


def get_rect_area(rect):
    return (rect[2] - rect[0]) * (rect[3] - rect[1])


def get_coords(b):
    point1 = (int(b[0]), int(b[1]))
    point2 = (int(b[2]), int(b[3]))
    return point1, point2, [int(b[0]), int(b[1]), int(b[2]), int(b[3])]


def get_center(rect):
    return int((rect[2] - rect[0]) / 2), int((rect[3] - rect[1]) / 2)


def get_shared_area(rect1, rect2):
    r1x1, r1y1, r1x2, r1y2 = rect1
    r2x1, r2y1, r2x2, r2y2 = rect2
    if rectangles_do_not_collide(rect1, rect2):
        return 0

    if r2x2 >= r1x2:
        horz = r1x2 - r2x1
    else:
        horz = r2x2 - r1x1

    if r2y2 >= r1y2:
        vert = r1y2 - r2y1
    else:
        vert = r2y2 - r1y1

    return horz * vert


def combine_rectangles(rect1, rect2):
    left = min(rect1[0], rect2[0])
    top = min(rect1[1], rect2[1])

    right = max(rect1[2], rect2[2])
    bottom = max(rect1[3], rect2[3])

    return [left, top, right, bottom]


def rect_dimensions(rect):
    width = rect[2] - rect[0]
    height = rect[3] - rect[1]
    return width, height


def calculate_distance_angle(origin_polygon_center, polygon_centers):
    info = []
    for polygon_center in polygon_centers:
        distance = math.sqrt(
            (origin_polygon_center[0] - polygon_center[0]) ** 2 + (origin_polygon_center[1] - polygon_center[1]) ** 2
        )
        angle = math.atan2(
            origin_polygon_center[1] - polygon_center[1],
            -1 * (origin_polygon_center[0] - polygon_center[0]),
        )
        info.append((distance, angle * 180 / 3.14))
    return info


def find_nearst_polygon(direction, origin_polygon, polygons, direction_threshold=5):
    origin_polygon_center = origin_polygon.get_center()
    polygon_centers = [get_center(polygon) for polygon in polygons]
    distance_angles = calculate_distance_angle(
        origin_polygon_center=origin_polygon_center, polygon_centers=polygon_centers
    )
    nearst_distance = None
    nearst_index = None
    for index, (distance, angle) in enumerate(distance_angles):
        if 0 < direction - direction_threshold:
            if angle < 0:
                angle += 360
        if not (direction - direction_threshold < angle < direction + direction_threshold):
            continue
        if nearst_distance is None or nearst_distance > distance:
            nearst_distance = distance
            nearst_index = index
    return nearst_index
