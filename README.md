mini project ini merupakan hasil eksperimen pembuatan composer dan data proc di GCP yang keduanya berbeda regional namun masih dalam 1 keluarga besar US (multiple regions in United States)

Region Composer: us-central1
Region DataProc: us-west1
Bucket Type : multiple regions in United States

cara kerja nya adalah ketika DAG dijalankan maka akan membuat cluster -> kemudian submit spark job ke cluster tersebut -> lalu delete cluster.
yang dilakukan dalam spark job tersebut adalah kita mengirim .jar ke cluster yg sudah dibuatkan dan menggunakan class JavaWordCount sebagai logic buat hitung kata-kata dari input.txt.
untuk hasil wordcount dapat dilihat di logs job nya ya akang teteh.

kurang lebih seperti itu, mohon maaf jika masih banyak kesalahan :)

note :
silakan membuat variable di airflow dan buat bucket multiple regions sesuai screenshot 