import cv2
import face_recognition

video_source = "sample_video.mp4"

# Read the movie and get the length
input_movie = cv2.VideoCapture(video_source)
length = int(input_movie.get(cv2.CAP_PROP_FRAME_COUNT))

# Create an output file with required resolution and frame rate as that of input file

# Load sample image of user to identify and track
image = face_recognition.load_image_file("sample_image.jpeg")
face_encoding = face_recognition.face_encoding(image)[0]
known_faces = [face_encoding,]

# Extract frame from video, find faces and identify them, create new video by overlaying original frame and location of speaker

face_locations = []
face_encodings = []
face_names = []
frame_number = 0

while True:
    #Grab a single frame from video

    ret, frame = input_movide.read()
    frame_number += 1

    # Quit when the input video file ends
    if not ret:
        break

    #Convert from BGR to RGB
    rgb_frame = frame[:,:, ::-1] # Width, Height, Color. Invert color order

    #Find all the faces and face encodings in the current frame of video
    face_locations = face_recognition.face_locations(rgb_frame, model="cnn")
    face_encodings = face_recognition.face_encodings(rgb_frame, face_locations)

    face_names = []

    for face_encoding in face_encodings:
        # See if the face is a match for the known faces
        match = face_recognition.compare_faces(known_faces, face_encoding, tolerance=0.50)

        name  = None
        if match[0]:
            name = "User 1"

        face_names.append(name)

    #Label the results
    for (top, right, bottom, left), name in zip(face_locations, face_names):
        if not name:
            continue

        # Draw a box around the face
        cv2.rectangle(frame, (left, top), (right, bottom), (0, 0, 255), 2)

        # Draw a label with a name below the face
        cv2.rectangle(frame, (left, bottom - 25), (right, bottom), (0, 0, 255), cv2.FILLED)

        font = cv2.FONT_HERSHEY_DUPLEX
        cv2.putText(frame, name, (left+6, bottom-6), font, 0.5, (255,255,255), 1)

    # Write the resulting image to the output video file
    print("Writing frame {} / {}".format(frame_number, length))

    output_movie.write(frame)

# All done!
input_movie.release()
cv2.destroyAllWindows()

