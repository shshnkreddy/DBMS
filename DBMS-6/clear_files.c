#include<stdio.h>

int main(){
    FILE* f1 = fopen("student.dat", "wb");
    FILE* f2 = fopen("student.ndx", "wb");
    FILE* f3 = fopen("course.dat", "wb");
    FILE* f4 = fopen("course.ndx", "wb");
    FILE* f6 = fopen("academia.db", "wb");
    FILE* f7 = fopen("student_course.lnk", "wb");

    fclose(f1);
    fclose(f2);
    fclose(f3);
    fclose(f4);
    fclose(f6);
    fclose(f7);
    return 0; 
}