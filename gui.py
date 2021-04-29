import tkinter as tk


# Sources:
# https://docs.python.org/3/library/tkinter.ttk.html#widget
# https://realpython.com/python-gui-tkinter/
# https://www.python-course.eu/tkinter_entry_widgets.php
# https://matplotlib.org/3.1.0/gallery/user_interfaces/embedding_in_tk_sgskip.html


def input_gui():
    user_inputs = None

    def get_inputs():
        nonlocal user_inputs
        user_inputs = {'intersect_fc': p1.get(), 'buf_distance': p2.get(), 'map_subtitle': p3.get()}

    gui = tk.Tk()
    gui.wm_title('West Nile Virus Simulation Inputs')
    tk.Label(gui, text='Intersect feature class name, example: IntersectAnalysis').grid(sticky=tk.W, row=0)
    p1 = tk.Entry(gui)
    p1.grid(row=0, column=1)
    tk.Label(gui, text='Buffer distance, example: 2500 Feet').grid(sticky=tk.W, row=1)
    p2 = tk.Entry(gui)
    p2.grid(row=1, column=1)
    tk.Label(gui, text='Map subtitle, example: 2500 Feet').grid(sticky=tk.W, row=2)
    p3 = tk.Entry(gui)
    p3.grid(row=2, column=1)
    tk.Button(gui, text='Submit', command=get_inputs).grid(row=3, column=0, sticky=tk.W, pady=4)
    tk.Button(gui, text='Quit', command=gui.quit).grid(row=3, column=1, sticky=tk.W, pady=4)
    gui.mainloop()

    return user_inputs


def main():
    user_inputs = input_gui()
    print(user_inputs)


if __name__ == '__main__':
    main()
