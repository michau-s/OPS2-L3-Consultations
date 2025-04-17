#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#define SHOP_FILENAME "./shop"
#define MIN_SHELVES 8
#define MAX_SHELVES 256
#define MIN_WORKERS 1
#define MAX_WORKERS 64

#define ERR(source)                                     \
    do                                                  \
    {                                                   \
        fprintf(stderr, "%s:%d\n", __FILE__, __LINE__); \
        perror(source);                                 \
        kill(0, SIGKILL);                               \
        exit(EXIT_FAILURE);                             \
    } while (0)

#define SWAP(x, y)         \
    do                     \
    {                      \
        typeof(x) __x = x; \
        typeof(y) __y = y; \
        x = __y;           \
        y = __x;           \
    } while (0)

typedef struct sharedData
{
    pthread_mutex_t mutexArr[MAX_SHELVES];
    int sorted;
    pthread_mutex_t mxSorted;
    int deadWorkerCount;
    pthread_mutex_t mxDeadWorkersCount;
} sharedData_t;

void usage(char* program_name)
{
    fprintf(stderr, "Usage: \n");
    fprintf(stderr, "\t%s n m\n", program_name);
    fprintf(stderr, "\t  n - number of items (shelves), %d <= n <= %d\n", MIN_SHELVES, MAX_SHELVES);
    fprintf(stderr, "\t  m - number of workers, %d <= m <= %d\n", MIN_WORKERS, MAX_WORKERS);
    exit(EXIT_FAILURE);
}

void msleep(unsigned int milli)
{
    time_t sec = (int)(milli / 1000);
    milli = milli - (sec * 1000);
    struct timespec ts = {0};
    ts.tv_sec = sec;
    ts.tv_nsec = milli * 1000000L;
    if (nanosleep(&ts, &ts))
        ERR("nanosleep");
}

void shuffle(int* array, int n)
{
    for (int i = n - 1; i > 0; i--)
    {
        int j = rand() % (i + 1);
        SWAP(array[i], array[j]);
    }
}

void print_array(int* array, int n)
{
    for (int i = 0; i < n; ++i)
    {
        printf("%3d ", array[i]);
    }
    printf("\n");
}

void mutex_lock_robust(sharedData_t* sharedData, int aisle)
{
    int ret;
    // mx Lock doesn't set errno, it returns the error code instead
    ret = pthread_mutex_lock(&sharedData->mutexArr[aisle]);
    if (ret == EOWNERDEAD)
    {
        printf("[%d] Found a dead body in aisle %d\n", getpid(), aisle);
        pthread_mutex_lock(&sharedData->mxDeadWorkersCount);
        sharedData->deadWorkerCount++;
        pthread_mutex_unlock(&sharedData->mxDeadWorkersCount);
        pthread_mutex_consistent(&sharedData->mutexArr[aisle]);
    }
}

void child_work(int* shopArr, int shopSize, sharedData_t* sharedData)
{
    int i;
    int j = -1;
    srand(getpid());

    while (1)
    {
        // Check if the array is sorted
        pthread_mutex_lock(&sharedData->mxSorted);
        if (sharedData->sorted)
        {
            pthread_mutex_unlock(&sharedData->mxSorted);
            break;
        }
        pthread_mutex_unlock(&sharedData->mxSorted);

        i = rand() % shopSize;
        while ((j = rand() % shopSize) == i)  // j index cannot be the same as i index
            ;

        if (j < i)
            SWAP(i, j);

        mutex_lock_robust(sharedData, i);
        mutex_lock_robust(sharedData, j);
        if (shopArr[i] > shopArr[j])
        {
            if (rand() % 100 == 0)
            {
                printf("[%d] Trips over pallet and dies\n", getpid());
                abort();
            }
            SWAP(shopArr[i], shopArr[j]);
            msleep(100);
        }

        pthread_mutex_unlock(&sharedData->mutexArr[i]);
        pthread_mutex_unlock(&sharedData->mutexArr[j]);
    }
}

void manager_work(int* shopArr, int shopSize, sharedData_t* sharedData, int workersCount)
{
    int sorted = 0;
    while (!sorted)
    {
        msync(shopArr, shopSize * sizeof(int), MS_SYNC);

        for (int i = 0; i < shopSize; i++)
        {
            mutex_lock_robust(sharedData, i);
        }

        print_array(shopArr, shopSize);

        sorted = 1;
        for (int i = 0; i < shopSize; i++)
        {
            if (shopArr[i] != i + 1)
            {
                sorted = 0;
                break;
            }
        }

        for (int i = 0; i < shopSize; i++)
        {
            pthread_mutex_unlock(&sharedData->mutexArr[i]);
        }

        pthread_mutex_lock(&sharedData->mxDeadWorkersCount);
        // The body is discovered twice, because each worker has two mutexes locked
        printf("[%d] Workers dead: %d\n", getpid(), sharedData->deadWorkerCount / 2);
        if (workersCount == sharedData->deadWorkerCount / 2)  // All workers have died
        {
            printf("[%d] All workers died, I hate my job\n", getpid());
            pthread_mutex_unlock(&sharedData->mxDeadWorkersCount);
            exit(EXIT_SUCCESS);
        }
        pthread_mutex_unlock(&sharedData->mxDeadWorkersCount);

        msleep(500);
    }

    printf("[%d] The shop shelves are sorted\n", getpid());
    pthread_mutex_lock(&sharedData->mxSorted);
    sharedData->sorted = 1;
    pthread_mutex_unlock(&sharedData->mxSorted);
    exit(EXIT_SUCCESS);
}

void createChildren(int workerCount, int* shopArr, int shopSize, sharedData_t* sharedData)
{
    int ret;
    for (int i = 0; i < workerCount; i++)
    {
        if (-1 == (ret = fork()))
            ERR("fork");

        if (ret == 0)  // Child
        {
            printf("[%d] Worker reports for a night shift\n", getpid());
            child_work(shopArr, shopSize, sharedData);

            // Cleanup
            munmap(shopArr, shopSize * sizeof(int));
            munmap(sharedData, sizeof(sharedData_t));
            exit(EXIT_SUCCESS);
        }
        // Parent
    }

    if (-1 == (ret = fork()))
        ERR("fork");
    if (ret == 0)  // Manager
    {
        printf("[%d] Manager reports for a night shift\n", getpid());
        manager_work(shopArr, shopSize, sharedData, workerCount);

        // Cleanup
        munmap(shopArr, shopSize * sizeof(int));
        munmap(sharedData, sizeof(sharedData_t));
        exit(EXIT_SUCCESS);
    }
}

int main(int argc, char** argv)
{
    if (argc != 3)
        usage(argv[0]);
    int productsCount, workersCount;

    productsCount = atoi(argv[1]);
    workersCount = atoi(argv[2]);

    if (productsCount < MIN_SHELVES || productsCount > MAX_SHELVES)
        usage(argv[0]);

    if (workersCount < MIN_WORKERS || workersCount > MAX_WORKERS)
        usage(argv[0]);

    int shopFd;
    if (-1 == (shopFd = open(SHOP_FILENAME, O_CREAT | O_RDWR | O_TRUNC, 0666)))
        ERR("open");

    if (-1 == ftruncate(shopFd, productsCount * sizeof(int)))
        ERR("ftruncate");

    int* shopArr;
    shopArr = mmap(NULL, productsCount * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, shopFd, 0);
    close(shopFd);  // We can close the file as soon as we map

    if (shopArr == MAP_FAILED)
        ERR("mmap");

    for (int i = 0; i < productsCount; i++)
    {
        shopArr[i] = i + 1;
    }

    sharedData_t* sharedData =
        mmap(NULL, sizeof(sharedData_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    sharedData->deadWorkerCount = 0;
    sharedData->sorted = 0;

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);

    pthread_mutex_init(&sharedData->mxSorted, &attr);
    pthread_mutex_init(&sharedData->mxDeadWorkersCount, &attr);

    for (int i = 0; i < MAX_SHELVES; i++)
    {
        pthread_mutex_init(&sharedData->mutexArr[i], &attr);
    }

    // We may destroy the attribute structure right away
    pthread_mutexattr_destroy(&attr);

    if (sharedData == MAP_FAILED)
        ERR("mmap");

    shuffle(shopArr, productsCount);
    print_array(shopArr, productsCount);
    createChildren(workersCount, shopArr, productsCount, sharedData);

    while (wait(NULL) > 0)
        ;

    print_array(shopArr, productsCount);
    printf("Night shift in Bitronka is over\n");

    // Cleanup
    for (int i = 0; i < MAX_SHELVES; i++)
    {
        pthread_mutex_destroy(&sharedData->mutexArr[i]);
    }
    pthread_mutex_destroy(&sharedData->mxSorted);
    pthread_mutex_destroy(&sharedData->mxDeadWorkersCount);

    // We need to sync to ensure that the contents actually get written back
    msync(shopArr, productsCount * sizeof(int), MS_SYNC);
    munmap(sharedData, sizeof(sharedData_t));
    munmap(shopArr, productsCount * sizeof(int));
    exit(EXIT_SUCCESS);
}
