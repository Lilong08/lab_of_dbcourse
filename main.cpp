/*
 * 数据库实验
 * 缓冲区管理和存储器管理实现
 * c++
 */

#include <iostream>
#include <string>
#include <cstdio>

#include <cstring>
#include <cassert>

using namespace std;

/*
 * 全局变量定义
 */
//	page 和 frame的大小为4KB
const int frame_size = 4096;
//  定义page 大小
const int page_size = 4096;
//	定义缓冲区大小为1024
const int def_bufsize = 1024;

//  文件包含最大block/pages数目
//  50000 data files and 100 directory files
const int max_pages = 50000;
//  定义文件目录
const char* file_path = "../files/data.dbf";


//  define two protecions
//  1 for write and 0 for read
const int w_prot = 1;
const int r_prot = 0;


//	frame结构定义
struct b_frame
{
    char field[frame_size];
};
//  申请大小为 1024*4KB的缓冲区
b_frame buf[def_bufsize];

/*-------------------------------------------------*/

/*
 * 缓冲区管理所需数据结构定义
 */
//	lur链表节点
struct lru_node
{
    int frame_id;
    lru_node* left = nullptr;
    lru_node* right = nullptr;
};

//	定义lru和mru指针
lru_node* lru = nullptr;
lru_node* mru = nullptr;


//	静态哈希函数
//	桶数量==buffer_size
//	桶的大小 == 1


struct BCB
{
    BCB(){};
    int page_id;
    int frame_id;

    //  no use for this lab
    int latch;
    //  no use for this lab
    int count;

    int dirty;
    //	溢出链
    BCB* next;
};

//	page_id和frame_id的映射
struct new_page
{
    int page_id;
    int frame_id;
};

/*-------------------------------------------------*/


/*
 * 文件存储管理器数据结构定义
 */

/*-------------------------------------------------*/

/*
 * 缓冲区管理类定义
 */
class BMgr
{
public:
    BMgr();
    //	Interface functions
    //	返回page_id对应的frame_id
    int FixPage(int page_id, int prot);
    void lru_init();

    //	返回{page_id : frame_id}的映射对
    //	void NewPage FixNewPage();
    new_page fix_newpage();
    int UnfixPage(int page_id);
    int NumFreeFrames();

    // Internal Functions
    int SelectVictim();
    int Hash(int page_id);
    void RemoveBCB(BCB * ptr, int page_id);
    void RemoveLRUEle(int frame_id);
    void SetDirty(int frame_id);
    void UnsetDirty(int frame_id);
    void WriteDirtys();
    void PrintFrame(int frame_id);
    ~BMgr();
private:
    //  Hash Table
    //  ftop map buffer id to page id
    //  ptof map page id to buffer id bucket
    int ftop[def_bufsize];
    BCB* ptof[def_bufsize];
};
/*-------------------------------------------------*/

/*
 * 数据存储管理器
 */

class DSMgr
{
public:
    DSMgr();
//    int OpenFile(string filename);
    int OpenFile(const char* filename);
    int CloseFile();
    b_frame ReadPage(int page_id);
//    int WritePage(int frame_id, b_frame frm);
    int WritePage(int page_id, b_frame frm);
    int Seek(int offset, int pos);
    FILE* GetFile();
    void IncNumPages();
    int GetNumPages();
    void SetUse(int index, int use_bit);
    int GetUse(int index);
    ~DSMgr();

private:
    FILE *currFile;
    int numPages;
    int pages[max_pages];
};
/*-------------------------------------------------*/


//  缓冲区管理和存储器管理定义
BMgr *bmgr = nullptr;
DSMgr *dsmgr = nullptr;

/*-------------------------------------------------*/

/*
 * 数据存储管理器接口实现
 */

//  open the data file
//  read file header
DSMgr::DSMgr()
{
    int err = OpenFile(file_path);
    if(err == -1)
    {
        cout<<"file open failed!\texit!"<<endl;
    }
    Seek(0, SEEK_SET);
    fread(&numPages, sizeof(int), 1, currFile);
    char bitmap_page[max_pages];
    fread(bitmap_page, max_pages, 1, currFile);
    for(int i = 0; i < max_pages; i++)
    {
        pages[i] = bitmap_page[i] - '0';
    }
    cout<<"creat a data storage manager!"<<endl;
}


int DSMgr::OpenFile(const char* filename)
{
    currFile = fopen(filename, "rb+");
    if(currFile == nullptr)    return -1;
    else return 0;
}

//  写回文件头信息
//  number of pages and bitmap for pages
//  关闭文件
int DSMgr::CloseFile()
{
    Seek(0, 0);
    fwrite(&numPages, sizeof(int), 1, currFile);
    int idx = 0;
    char bit[1];
    while(idx < max_pages)
    {
        bit[0] = pages[idx] + '0';
        fwrite(bit, 1, 1, currFile);
        idx++;
    }
    cout<<"file head write back!"<<endl;
    int return_code = fclose(currFile);
    return return_code;
}
int DSMgr::Seek(int offset, int pos)
{
    fseek(currFile, pos, SEEK_SET);
    fseek(currFile, offset, SEEK_CUR);
    return 1;
}

b_frame DSMgr::ReadPage(int page_id)
{

    //  todo:how to organize the directory
    //   how to lookup the directory
    //  ---------changed code-----------------
    int offset = -1;
    int ptr_start = ((max_pages*1 + 1*4 -1)/page_size + 1) * page_size;
    Seek(page_id*4, ptr_start);
    fread(&offset, sizeof(int), 1, currFile);

    //  ---------changed code-----------------

    Seek(offset, SEEK_SET);
    auto *frame = new b_frame;
    int ret = fread(frame, sizeof(b_frame), 1, currFile);
    assert(ret == 1);
    return *frame;

}
int DSMgr::WritePage(int page_id, b_frame frm)
{
    //  lookup the directory to get the page offset
    //  write data
    //  todo:how to organize the directory
    //   how to lookup the directory


    //  fixed code
    int offset = -1;
    int ptr_start = ((max_pages*1 + 1*4 -1)/page_size + 1) * page_size;
    Seek(page_id*4, ptr_start);
    fread(&offset, sizeof(int), 1, currFile);

    //  write back buffer to the page

    Seek(offset, SEEK_SET);
    const b_frame* w_frame = &frm;
    int ret = fwrite(w_frame, sizeof(b_frame), 1, currFile);
    assert(ret == 1);
    return sizeof(w_frame);
}

FILE* DSMgr::GetFile()
{
    return currFile;
}

void DSMgr::IncNumPages()
{
    int tmp = numPages;
    tmp++;
    numPages = tmp;
}
int DSMgr::GetNumPages()
{
    int tmp = numPages;
    return tmp;
}
void DSMgr::SetUse(int index, int use_bit)
{
    //  index == page_id
    //  负责写 index page 的偏移指针
    if(pages[index] == 0 && use_bit == 1)
    {
        int ptr_start = ((max_pages*1 + 1*4 -1)/page_size + 1) * page_size;
        int offset = ((max_pages*4 - 1)/page_size + 1)*page_size + index*page_size + ptr_start;
        Seek(index * 4, ptr_start);
        fwrite(&offset, sizeof(int), 1, currFile);
    }

    pages[index] = use_bit;
}
int DSMgr::GetUse(int index)
{
    int use = pages[index];
    return use;
}

DSMgr::~DSMgr()
{
    int error = CloseFile();
    if(error == EOF)
    {
        cout<<"file colses failed!"<<endl;
    }
    cout<<"data storage manager exit!"<<endl;
}

/*-------------------------------------------------*/



/*
 * 缓冲管理器接口实现
 */
BMgr::BMgr()
{
    lru_init();
    for(int i = 0; i < def_bufsize; i++)
    {
        ftop[i] = -1;
        ptof[i] = new BCB;
        ptof[i]->next = nullptr;
    }
    cout<<"creat a buffer manager!"<<endl;
}

void BMgr::lru_init()
{
    lru = new lru_node;
    mru = new lru_node;
    lru->left = nullptr;
    lru->right = mru;
    lru->frame_id = -1;
    mru->left = lru;
    mru->right = nullptr;
    mru->frame_id = -1;
}

//  whti no directory
new_page BMgr::fix_newpage()
{
    //  call for creat table or page(record) insert
    //  put the inserting page in a buffer and
    //  return the pair of page id and buffer id
    int no_used_page = -1;
    int no_used_buffer = -1;

    for (int i = 0; i < max_pages; i++) {
        if (dsmgr->GetUse(i) == 0) {
            no_used_page = i;
            break;
        }
    }
    dsmgr->SetUse(no_used_page, 1);
    dsmgr->IncNumPages();
        //  todo:write the page to the file, also modify the directory block
        //   this happens when the buffer selected as a victim
        //    this function need not any writting operation
    for(int i = 0; i < def_bufsize; i++)
    {
        if(ftop[i] == -1)
        {
            no_used_buffer = i;
            break;
        }
    }
    if(no_used_buffer == -1)
    {
        no_used_buffer = SelectVictim();
        //  todo:write the victim frame back to the file
        //   do this in the BCBremove function
    }

    ftop[no_used_buffer] = no_used_page;

    BCB* head = ptof[no_used_page%def_bufsize];
    BCB* tmp = new BCB;
    tmp->frame_id = no_used_buffer;
    tmp->page_id = no_used_page;
    tmp->next = nullptr;
    tmp->count = 0;
//    tmp->dirty = 1;
    if(head ->next ==  nullptr) head->next = tmp;
    else
    {
        tmp->next = head->next;
        head->next = tmp;
    }
    SetDirty(no_used_buffer);
    auto* n = new new_page;
    n->page_id = no_used_page;
    n->frame_id = no_used_buffer;


    //  update the lru link list
    auto *node = new lru_node;
    int __frame_id = no_used_buffer;
    node->frame_id = __frame_id;
    auto *p = mru;
    while(p != nullptr && p->frame_id != __frame_id)    p = p->left;
    if(p == nullptr)
    {
        mru->left->right = node;
        node->left = mru->left;
        node->right = mru;
        mru->left = node;
    }
    else
    {
        p->left->right = p->right;
        p->right->left = p->left;
        mru->left->right = p;
        p->left = mru->left;
        p->right = mru;
        mru->left = p;
    }
    return *n;
}

//
//new_page BMgr::fix_newpage()
//{
//    //  call for creat table or page(record) insert
//    //  put the inserting page in a buffer and
//    //  return the pair of page id and buffer id
//    int no_used_page = -1;
//    int no_used_buffer = -1;
//    if(dsmgr->GetUse(file_block_zero) == 0)
//    {
//        dsmgr->SetUse(file_block_zero, 1);
//        dsmgr->IncNumPages();
//    }
//    else
//    {
//        for(int i = 0; i < max_pages; i++)
//        {
//            if(dsmgr->GetUse(i) == 0)
//            {
//                no_used_page = i;
//                break;
//            }
//        }
//        dsmgr->SetUse(no_used_page, 1);
//        dsmgr->IncNumPages();
//        //  todo:write the page to the file, also modify the directory block
//        //   this happens when the buffer selected as a victim
//        //    this function need not any writting operation
//    }
//    for(int i = 0; i < def_bufsize; i++)
//    {
//        if(ftop[i] == -1)
//        {
//            no_used_buffer = i;
//            break;
//        }
//    }
//    if(no_used_buffer == -1)
//    {
//        no_used_buffer = SelectVictim();
//        //  todo:write the victim frame back to the file
//        int cur_page = ftop[no_used_buffer];
//        BCB* ptr = ptof[cur_page%def_bufsize];
//        while(ptr->next != nullptr && ptr->next->page_id != cur_page)  ptr = ptr->next;
//        if(ptr->next->dirty == w_prot)
//        {
//            dsmgr->WritePage(cur_page, buf[no_used_buffer]);
//        }
//    }
//
//    ftop[no_used_buffer] = no_used_page;
//
//    BCB* head = ptof[no_used_page%def_bufsize];
//    BCB* tmp = new BCB;
//    tmp->frame_id = no_used_buffer;
//    tmp->page_id = no_used_page;
//    tmp->next = nullptr;
//    tmp->count = 0;
////    tmp->dirty = 1;
//    if(head ->next ==  nullptr) head->next = tmp;
//    else
//    {
//        tmp->next = head->next;
//        head->next = tmp;
//    }
//    SetDirty(no_used_buffer);
//    auto* n = new new_page;
//    n->page_id = no_used_page;
//    n->frame_id = no_used_buffer;
//    return *n;
//}

int BMgr::FixPage(int page_id, int prot)
{
    //  protection 0 for read 1 for write the page
    int __frame_id = Hash(page_id);
    if(__frame_id != -1)
    {
        //  page is in the buffer
        int cur_page = ftop[__frame_id];
        BCB* head = ptof[cur_page%def_bufsize];
        while(head->next != nullptr && head->next->page_id != cur_page)  head = head->next;
//        head->next->dirty = prot;
        //  if use SetDirty() function
        if(prot == w_prot)   SetDirty(head->next->frame_id);
        cout<<"buffer hit!"<<endl;
    }
    if(__frame_id == -1)
    {
        //  page is not in the buffer
        cout<<"page "<<page_id<<" is not in the buffer"<<endl;
        int num_of_free = NumFreeFrames();
        if(num_of_free == 0)
        {
            //  todo:
            //  conduct the replace procedure
            //  and get the frame_id
            __frame_id = SelectVictim();
            //  todo:write the victim frame to the file
            //   do it in the BCBremove function
        }
        else
        {
            //  get a free frame
            //  read the page to the frame
            for(int i = 0; i < def_bufsize; i++)
            {
                if(ftop[i] == -1)
                {
                    __frame_id = i;
                    break;
                }
            }
        }
        auto* free_frame = &buf[__frame_id];
        b_frame r_frame = dsmgr->ReadPage(page_id);
        *free_frame = r_frame;

        BCB *tmp = new BCB;
        tmp->frame_id = __frame_id;
        tmp->page_id = page_id;
        tmp->next = nullptr;
        tmp->count = 0;
//        tmp->dirty = prot;

        ftop[__frame_id] = page_id;

        //  link-list insert from the head
        BCB* head = ptof[page_id%def_bufsize];
        if(head->next == nullptr) head->next = tmp;
        else
        {
            tmp->next = head->next;
            head->next = tmp;
        }
        //  if use SetDirty() function
        if(prot == w_prot)    SetDirty(tmp->frame_id);
    }

    //  update the lru link list
    auto *node = new lru_node;
    node->frame_id = __frame_id;
    auto *p = mru;
    while(p != nullptr && p->frame_id != __frame_id)    p = p->left;
    if(p == nullptr)
    {
        mru->left->right = node;
        node->left = mru->left;
        node->right = mru;
        mru->left = node;
    }
    else
    {
        p->left->right = p->right;
        p->right->left = p->left;
        mru->left->right = p;
        p->left = mru->left;
        p->right = mru;
        mru->left = p;
    }

    return __frame_id;
}

int BMgr::NumFreeFrames()
{
    int nums = 0;
    for(auto& i : ftop)
    {
        if(i == -1) nums++;
    }
    return nums;
}

int BMgr::UnfixPage(int page_id)
{
    //  todo:fix this function
    //  if FixPage(page_id) is called, then
    //  this function is called to decrement the count of the pages in the buffer
    //  except page page_id

    return -1;
}

int BMgr::SelectVictim()
{
    //  todo:fix the function
    //  just return the lru->frame_id is ok
    int victim_frame_id = lru->right->frame_id;
    int page_id = ftop[victim_frame_id];
    BCB* head = ptof[page_id%def_bufsize];
    RemoveBCB(head, page_id);
    RemoveLRUEle(victim_frame_id);
    if(head->next == nullptr)   ftop[victim_frame_id] = -1;
    return victim_frame_id;
}

void BMgr::RemoveBCB(BCB *ptr, int page_id)
{
    //  write back page in this function
    BCB* vic_f;
    while(ptr->next != nullptr && ptr->next->page_id != page_id)  ptr = ptr->next;
    vic_f = ptr->next;
    ptr->next = ptr->next->next;
    if(vic_f->dirty == w_prot)
    {
        dsmgr->WritePage(page_id, buf[vic_f->frame_id]);
        cout<<"dirty page " <<page_id<<" has write back to the file done!"<<endl;
    }
    delete(vic_f);
    cout<<"BCB removed!"<<endl;
}

void BMgr::RemoveLRUEle(int frame_id)
{
    lru_node* tmp = lru->right;
    assert(tmp->frame_id == frame_id);
    lru->right = tmp->right;
    tmp->right->left = lru;
    delete(tmp);
    cout<<"LRUEle removed!"<<endl;
}

int BMgr::Hash(int page_id)
{
    //  完成page_id 到 frame_id 的映射
    //  返回-1则映射失败
    int frame_id = -1;
    int idx = (page_id)%(def_bufsize);
    BCB *p = ptof[idx];

    while(p->next != nullptr && p->next->page_id != page_id)    p = p->next;
    if(p ->next != nullptr)
        frame_id = p->next->frame_id;
    return frame_id;
}

void BMgr::SetDirty(int frame_id)
{
    int page = ftop[frame_id];
    BCB* ptr = ptof[page%def_bufsize];
    while(ptr->next != nullptr && ptr->next->page_id != page) ptr = ptr->next;
    ptr->next->dirty = w_prot;
}

void BMgr::UnsetDirty(int frame_id)
{
    cout<<"no use!"<<endl;
}

void BMgr::WriteDirtys()
{
    //  lookup all the BCB to see if a frame needed to write back
    BCB* ptr;
    int p_id;
    int f_id;
    for(int i = 0; i < def_bufsize; i++)
    {
        ptr = ptof[i];
        while(ptr->next != nullptr)
        {
            if (ptr->next->dirty == w_prot)
            {
                p_id = ptr->next->page_id;
                f_id = ptr->next->frame_id;
                dsmgr->WritePage(p_id, buf[f_id]);
            }
            ptr = ptr->next;
        }
    }
    cout<<"all the dirty pages write back!"<<endl;
}
void BMgr::PrintFrame(int frame_id)
{
    cout<<buf[frame_id].field<<endl;
}


BMgr::~BMgr()
{
    cout<<"buffuer manager exit!"<<endl;
}

/*-------------------------------------------------*/


/*
 * 堆文件构建函数
 *

 * 建立50000个page文件
 *
 * 文件结构为：
 *         开始为4字节的int型常量：该文件中的page的数量
 *         接着为50000个page的0、1位示图
 *         然后是偏移指针区域，每个page的偏移量
 */

void init_file()
{
    //  初始化文件头
    FILE *fp = fopen(file_path, "w");
    int num_of_pages = 0;
    fwrite(&num_of_pages, sizeof(int), 1, fp);
    char no_use[1] = {'0'};
    int tmp_num = max_pages;
    while(tmp_num--)
    {
        fwrite(no_use, 1, 1, fp);
    }
    fclose(fp);
}
void creat_file()
{

    int max_page_nums = 50000;
    b_frame* f = new b_frame;
    strcpy(f->field, "this is a set of records");
    new_page* tmp = new new_page;
    int cnt = 0;
    while(max_page_nums--)
    {
        *tmp = bmgr->fix_newpage();
        buf[tmp->frame_id] = *f;
        cnt++;
        if(cnt % 5000 == 0)    cout<<"page "<<cnt<<" insert!"<<endl;
    }
    cout<<"heap file construct success!"<<endl;
    delete(f);
    delete(tmp);
}

/*-------------------------------------------------*/

int main() {

//    init_file();
    bmgr = new BMgr;

    dsmgr = new DSMgr;
//    creat_file();

    b_frame* f = new b_frame;
    *f = dsmgr->ReadPage(10);
    cout<<f->field<<endl;
    for(int i = 0; i < 1024; i++)
    {
        bmgr->FixPage(i, i%2);
    }
    for(int i = 1010; i < 1044; i++)
    {
        bmgr->FixPage(i, i%2);
    }
    delete(bmgr);
    delete(dsmgr);
    return 0;
}