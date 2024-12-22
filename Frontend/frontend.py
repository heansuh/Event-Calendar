# Frontend implementation built by: Heansuh Lee

import tkinter as tk
from tkinter import ttk, messagebox
from PIL import Image, ImageTk

# Sample data for demonstration with dummy images
sample_events = [
    {"city": "Berlin", "event": "Tech Conference", "date": "2024-11-05", "image": "sample1.jpg"},
    {"city": "Hamburg", "event": "Art Expo", "date": "2024-11-06", "image": "sample2.jpg"},
    {"city": "Munich", "event": "Startup Summit", "date": "2024-11-10", "image": "sample3.jpg"},
]

class EventSchedulerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Event Scheduler")
        self.root.geometry("600x600")
        self.root.configure(bg="#f5f5f5")
        
        # Title
        title_label = tk.Label(root, text="Event Scheduler", font=("Helvetica", 18, "bold"), bg="#f5f5f5", fg="#333")
        title_label.pack(pady=10)

        # Create buttons with styles
        style = ttk.Style()
        style.configure("TButton", font=("Helvetica", 12), padding=6)
        
        self.check_data_btn = ttk.Button(root, text="Check Data", command=self.check_data, style="TButton")
        self.check_data_btn.pack(pady=10)
        
        self.send_to_calendar_btn = ttk.Button(root, text="Send to Google Calendar", command=self.send_to_calendar, style="TButton")
        self.send_to_calendar_btn.pack(pady=10)

        # Frame to hold event checkboxes and images
        self.event_frame = tk.Frame(root, bg="#f5f5f5")
        self.event_frame.pack(pady=10, fill='both', expand=True)

    def check_data(self):
        # Clear existing widgets
        for widget in self.event_frame.winfo_children():
            widget.destroy()
        
        self.event_vars = []
        for event in sample_events:
            event_var = tk.IntVar()
            
            # Event frame
            frame = tk.Frame(self.event_frame, bg="white", relief="groove", bd=1)
            frame.pack(padx=10, pady=5, fill='x')
            
            # Event image
            try:
                img = Image.open(event["image"])
                img = img.resize((60, 60), Image.ANTIALIAS)
                img_tk = ImageTk.PhotoImage(img)
                image_label = tk.Label(frame, image=img_tk, bg="white")
                image_label.image = img_tk  # Keep a reference
                image_label.grid(row=0, column=0, rowspan=2, padx=10, pady=5)
            except:
                pass  # If image fails to load, it skips showing

            # Event details
            event_info = f"{event['event']} in {event['city']} on {event['date']}"
            info_label = tk.Label(frame, text=event_info, font=("Helvetica", 12), bg="white", anchor="w")
            info_label.grid(row=0, column=1, sticky="w")
            
            # Event checkbox
            chk = tk.Checkbutton(frame, text="Select", variable=event_var, bg="white", font=("Helvetica", 10))
            chk.grid(row=1, column=1, sticky="w")
            
            self.event_vars.append((event_var, event))

        messagebox.showinfo("Data Loaded", "Sample data loaded. Select events to send to Google Calendar.")

    def send_to_calendar(self):
        # Get selected events
        selected_events = [event for var, event in self.event_vars if var.get() == 1]
        
        if not selected_events:
            messagebox.showwarning("No Selection", "Please select at least one event to send.")
            return
        
        # Simulate sending to Google Calendar
        event_list = "\n".join([f"{event['city']}: {event['event']} on {event['date']}" for event in selected_events])
        messagebox.showinfo("Send to Calendar", f"Sending the following events to Google Calendar:\n\n{event_list}")

# Main application
root = tk.Tk()
app = EventSchedulerApp(root)
root.mainloop()
